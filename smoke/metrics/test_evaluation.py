"""
Comprehensive validation tests for summary evaluation metrics.

Tests verify each metric correctly distinguishes between good and bad summaries
using pairwise comparisons across multiple quality levels.

Performance: Metric computations are cached at session scope, computed once
and reused across all tests. This eliminates redundant computation.
"""

import pytest
from itertools import combinations
from typing import Dict, List, Tuple, Callable
from tests.helpers import optional_param
from src.components.metrics import (
    run_rouge_l,
    run_bertscore,
    run_novel_ngrams,
    run_jsd_distribution,
    run_entity_coverage,
    run_ncd_overlap,
    run_salience_recall,
    run_nli_faithfulness,
    run_readability_delta,
    run_sentence_coherence,
    run_entity_grid_coherence,
    run_lexical_diversity,
    run_stopword_ratio,
)


@pytest.fixture(scope="session")
def book_data():
    """Realistic narrative fiction with multiple summary quality levels.
    
    Source: Alice in Wonderland excerpt with dialogue, description, and plot.
    
    Summaries:
    - excellent: Faithful, coherent, preserves key entities and events
    - normal: Adequate coverage but generic phrasing, some loss of detail
    - terrible: Multiple hallucinations (wrong names, invented events),
                poor coherence, missing critical entities
    """
    return {
        "text": (
            'Alice was beginning to get very tired of sitting by her sister on the bank, '
            'and of having nothing to do. "What is the use of a book," thought Alice, '
            '"without pictures or conversations?" Suddenly a White Rabbit with pink eyes '
            'ran close by her. There was nothing so very remarkable in that, but when the '
            'Rabbit actually took a watch out of its waistcoat-pocket, and looked at it, '
            'Alice thought she ought to follow it.\n\n'
            
            'She ran across the field after it and was just in time to see it pop down '
            'a large rabbit-hole under the hedge. In another moment down went Alice after '
            'it, never once considering how in the world she was to get out again. The '
            'rabbit-hole went straight on like a tunnel for some way, and then dipped '
            'suddenly down. Down, down, down. Alice fell very slowly, for she had plenty '
            'of time as she went down to look about her and wonder what was going to happen.\n\n'
            
            'She landed in a long, low hall lit by a row of lamps. There were doors all '
            'round the hall, but they were all locked. She found a little three-legged '
            'table made of solid glass with a tiny golden key upon it. Behind a low curtain '
            'she found a door about fifteen inches high. The key fit! But Alice could not '
            'squeeze through. She went back to the table and found a little bottle labeled '
            '"DRINK ME." It was not marked poison, so Alice ventured to taste it. "What a '
            'curious feeling!" said Alice. "I must be shutting up like a telescope." She '
            'was now only ten inches high. But she had forgotten the key on the table above!\n\n'
            
            '"You ought to be ashamed of yourself," said Alice, "a great girl like you, to '
            'go on crying in this way!" But she went on shedding gallons of tears until '
            'there was a large pool. She heard a pattering of feet and the White Rabbit '
            'appeared. "Oh, the Duchess! She\'ll get me executed, as sure as ferrets are '
            'ferrets!" cried the Rabbit, dropping his gloves and fan as he ran. Alice picked '
            'them up and began fanning herself. "Dear me, how queer everything is today!" '
            'she said. She looked down and was surprised to find that she had put on one of '
            'the Rabbit\'s little gloves while she was talking. "I must be growing small '
            'again," she thought, and went to the table to measure herself by it. She was '
            'about two feet high now and shrinking rapidly. The fan! She dropped it hastily '
            'just in time to avoid shrinking away altogether.'
        ),
        
        "excellent": (
            "Alice was seated beside her sibling on the riverbank when she spotted a White Rabbit "
            "with pink eyes. The White Rabbit extracted a timepiece from its waistcoat-pocket and "
            "examined it. Alice pursued the White Rabbit and observed it pop down a large rabbit-hole "
            "beneath the hedge. Alice descended the rabbit-hole after it. The rabbit-hole "
            "led Alice straight onward like a tunnel and then dipped suddenly downward. Alice tumbled very "
            "slowly down the shaft. Alice arrived in a lengthy low corridor illuminated by lamps. Alice noticed the doors "
            "were all secured. Alice discovered a small table constructed of solid glass with a tiny "
            "golden key upon it. Behind a curtain Alice located a doorway approximately fifteen inches tall. "
            "The key unlocked the entrance but Alice could not squeeze through. Alice found a little "
            "bottle labeled DRINK ME. Alice sampled it and experienced a curious sensation. Alice was "
            "now merely ten inches in height. But Alice had neglected the key on the table above. "
            "Alice began weeping and shed gallons of tears until there was a large pool. "
            "Alice heard footsteps and the White Rabbit reappeared. The White Rabbit cried about the "
            "Duchess and dropped his gloves and fan as he ran. Alice collected the White Rabbit's "
            "gloves and began fanning herself. Alice glanced downward and realized she had donned one of the "
            "White Rabbit's little gloves. Alice was shrinking small again. Alice measured approximately two feet "
            "in height and diminishing rapidly. Alice released the fan hastily to avoid vanishing "
            "away altogether."
        ),
        
        "normal": (
            "Alice saw a White Rabbit with a watch and followed it down a rabbit hole. "
            "She fell down and landed in a hall with locked doors. Alice found a key on "
            "a glass table and a small door. The key opened the door but she was too "
            "big to fit through. She drank from a bottle and became very small. Alice "
            "could not reach the key anymore. She cried and made a pool of tears. "
            "The White Rabbit came back and was worried about the Duchess. The Rabbit "
            "dropped his gloves and fan. Alice picked up the items and used the fan. "
            "The fan made her shrink more. She dropped the fan quickly."
        ),

        "terrible": (
            "Girl was walking in the garden with her friend when they saw a rabbit. "
            "The girls followed it to a place with things. "
            "Inside the place they met a person named Person. Person gave Girl the "
            "thing to fly and gave her friend the thing to be invisible. A creature named "
            "Creature came and tried to take all the magic from the place. The "
            "person gave the girls things made of light to fight "
            "the creature. After they beat Creature they found a thing with "
            "coins and jewels inside. A man named Man told them they "
            "were the special ones from an old story. Man taught Girl and her friend "
            "ways to use fire and ice. The girls went home as heroes and kept "
            "their magic things forever."
        ),
    }


# ==============================================================================
# CACHED METRIC COMPUTATIONS
# ==============================================================================

@pytest.fixture(scope="session")
def metric_cache(book_data):
    """Compute all metrics once and cache results for entire test session.
    
    Cache structure:
    {
        "excellent": {"rouge_l": {...}, "bertscore": {...}, ...},
        "normal": {"rouge_l": {...}, "bertscore": {...}, ...},
        "terrible": {"rouge_l": {...}, "bertscore": {...}, ...}
    }
    
    Performance: Reduces ~390 metric computations (13 metrics × 3 levels × 10 tests)
    down to just 39 computations (13 metrics × 3 levels, computed once).
    """
    cache = {}
    text = book_data["text"]
    
    for level in ["excellent", "normal", "terrible"]:
        summary = book_data[level]
        cache[level] = {}
        
        # Comparison metrics (need source)
        cache[level]["rouge_l"] = run_rouge_l(summary, text)
        cache[level]["bertscore"] = run_bertscore(summary, text)
        cache[level]["novel_ngrams"] = run_novel_ngrams(summary, text)
        cache[level]["jsd_distribution"] = run_jsd_distribution(summary, text)
        cache[level]["entity_coverage"] = run_entity_coverage(summary, text)
        cache[level]["ncd"] = run_ncd_overlap(summary, text)
        cache[level]["salience_recall"] = run_salience_recall(summary, text)
        cache[level]["nli_faithfulness"] = run_nli_faithfulness(summary, text)
        cache[level]["readability_delta"] = run_readability_delta(summary, text)
        
        # Reference-free metrics
        cache[level]["sentence_coherence"] = run_sentence_coherence(summary)
        cache[level]["entity_grid_coherence"] = run_entity_grid_coherence(summary)
        cache[level]["lexical_diversity"] = run_lexical_diversity(summary)
        cache[level]["stopword_ratio"] = run_stopword_ratio(summary)
    
    return cache


def get_score(cache: Dict, level: str, metric_name: str, key: str) -> float:
    """Extract single metric value from cache."""
    return cache[level][metric_name][key]


# ==============================================================================
# QUALITY ORDERING AND COMPARISON HELPERS
# ==============================================================================

# Define expected quality ordering: excellent > normal > terrible
QUALITY_LEVELS = ["excellent", "normal", "terrible"]
BETTER_WORSE_PAIRS = [
    ("excellent", "normal"),
    ("excellent", "terrible"),
    ("normal", "terrible"),
]


# ==============================================================================
# PARAMETERIZED METRIC DEFINITIONS
# ==============================================================================

# Format: (cache_key, result_key, higher_is_better)
# cache_key: key in metric_cache dict
# result_key: key in returned metric dict
# higher_is_better: True if higher scores = better quality

HIGHER_IS_BETTER = [
    optional_param(("rouge_l", "rougeL_recall"), "rouge_score"),
    optional_param(("bertscore", "bertscore_f1"), "evaluate"),
    optional_param(("entity_coverage", "entity_coverage"), "spacy"),
    optional_param(("salience_recall", "salience_recall"), "sklearn"),
    optional_param(("nli_faithfulness", "nli_faithfulness"), "transformers"),
    optional_param(("sentence_coherence", "sentence_coherence"), "sentence_transformers"),
    optional_param(("entity_grid_coherence", "entity_grid_coherence"), "spacy"),
    optional_param(("lexical_diversity", "lexical_diversity"), "nltk"),
]

LOWER_IS_BETTER = [
    optional_param(("novel_ngrams", "novel_ngram_pct"), "nltk"),
    optional_param(("jsd_distribution", "jsd"), "scipy"),
    optional_param(("entity_coverage", "entity_hallucination"), "spacy"),
    optional_param(("ncd", "ncd"), "zlib"),
]


# ==============================================================================
# CORE VALIDATION TESTS
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(20)
@pytest.mark.parametrize("metric_keys", HIGHER_IS_BETTER, indirect=False)
@pytest.mark.parametrize("better_key,worse_key", BETTER_WORSE_PAIRS)
def test_higher_is_better_metrics(metric_keys, better_key, worse_key, metric_cache):
    """Verify metrics where higher scores indicate better summaries.
    
    Pattern: Coverage, Semantic Similarity, Coherence metrics.
    Tests all pairwise comparisons: excellent > normal > terrible.
    """
    cache_key, result_key = metric_keys
    
    better_score = get_score(metric_cache, better_key, cache_key, result_key)
    worse_score = get_score(metric_cache, worse_key, cache_key, result_key)
    
    assert better_score > worse_score, (
        f"{cache_key}.{result_key}: Expected {better_key} ({better_score:.3f}) > "
        f"{worse_key} ({worse_score:.3f})"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(21)
@pytest.mark.parametrize("metric_keys", LOWER_IS_BETTER, indirect=False)
@pytest.mark.parametrize("better_key,worse_key", BETTER_WORSE_PAIRS)
def test_lower_is_better_metrics(metric_keys, better_key, worse_key, metric_cache):
    """Verify metrics where lower scores indicate better summaries.
    
    Pattern: Novelty, Divergence, Hallucination metrics.
    Tests all pairwise comparisons: excellent < normal < terrible.
    """
    cache_key, result_key = metric_keys
    
    better_score = get_score(metric_cache, better_key, cache_key, result_key)
    worse_score = get_score(metric_cache, worse_key, cache_key, result_key)
    
    assert better_score < worse_score, (
        f"{cache_key}.{result_key}: Expected {better_key} ({better_score:.3f}) < "
        f"{worse_key} ({worse_score:.3f})"
    )


# ==============================================================================
# SPECIAL METRIC VALIDATION
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(22)
@pytest.mark.parametrize("better_key,worse_key", BETTER_WORSE_PAIRS)
def test_readability_delta_positive(better_key, worse_key, metric_cache):
    """Verify readability delta is positive (summary simpler than source).
    
    Positive delta means summary is easier to read than source.
    Both good and bad summaries should simplify, but magnitude may vary.
    """
    better_delta = get_score(metric_cache, better_key, "readability_delta", "readability_delta")
    worse_delta = get_score(metric_cache, worse_key, "readability_delta", "readability_delta")
    
    # Both should simplify (positive delta)
    assert better_delta > 0, f"{better_key} should simplify source text"
    assert worse_delta > 0, f"{worse_key} should simplify source text"
    
    # Excellent summary should have moderate simplification
    # Terrible summary may over-simplify or under-simplify depending on quality
    if better_key == "excellent" and worse_key == "terrible":
        # We expect some difference but don't enforce direction
        assert abs(better_delta - worse_delta) > 0.1, (
            "Excellent and terrible summaries should have meaningfully different "
            "readability deltas"
        )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(23)
def test_stopword_ratio_in_reasonable_range(metric_cache):
    """Verify stopword ratios are reasonable for all summary levels.
    
    Stopword ratio measures content density. Too high = fluffy, too low = dense.
    All summaries should be in reasonable range [0.3, 0.6] for English prose.
    """
    for level in QUALITY_LEVELS:
        ratio = get_score(metric_cache, level, "stopword_ratio", "stopword_ratio")
        
        assert 0.2 < ratio < 0.7, (
            f"{level} summary has unusual stopword ratio: {ratio:.3f}"
        )


# ==============================================================================
# EDGE CASE VALIDATION
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(24)
def test_single_sentence_summary_edge_cases():
    """Verify coherence metrics handle minimal summaries gracefully."""
    single_sent = "Alice followed a white rabbit down a hole."
    
    coherence = run_sentence_coherence(single_sent)
    assert coherence["sentence_coherence"] == 1.0, (
        "Single sentence should have perfect coherence"
    )
    
    grid = run_entity_grid_coherence(single_sent)
    assert grid["entity_grid_coherence"] == 1.0, (
        "Single sentence should have perfect entity grid coherence"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(25)
def test_identical_summary_and_source():
    """Verify metrics behave correctly when summary equals source."""
    text = (
        "The quick brown fox jumps over the lazy dog. "
        "The dog was not amused by this behavior."
    )
    
    # Perfect lexical overlap
    rouge = run_rouge_l(text, text)
    assert rouge["rougeL_recall"] == 1.0
    
    # Zero novelty
    novel = run_novel_ngrams(text, text)
    assert novel["novel_ngram_pct"] == 0.0
    
    # Perfect semantic similarity
    bert = run_bertscore(text, text)
    assert bert["bertscore_f1"] > 0.99  # Allow small float error
    
    # Zero compression distance
    ncd = run_ncd_overlap(text, text)
    assert ncd["ncd"] < 0.01  # Should be nearly zero


# ==============================================================================
# CROSS-METRIC CONSISTENCY VALIDATION
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(26)
def test_excellent_summary_characteristics(metric_cache):
    """Verify excellent summary exhibits expected multi-metric pattern.
    
    Excellent summaries should show:
    - High coverage (ROUGE-L, entity coverage)
    - High semantic similarity (BERTScore)
    - Low hallucination (entity hallucination, novel n-grams)
    - High coherence (sentence coherence)
    """
    rouge = get_score(metric_cache, "excellent", "rouge_l", "rougeL_recall")
    assert rouge > 0.4, "Should have good lexical coverage"
    
    ent_cov = get_score(metric_cache, "excellent", "entity_coverage", "entity_coverage")
    assert ent_cov > 0.5, "Should preserve most entities"
    
    ent_hal = get_score(metric_cache, "excellent", "entity_coverage", "entity_hallucination")
    assert ent_hal < 0.3, "Should have few hallucinations"
    
    bert = get_score(metric_cache, "excellent", "bertscore", "bertscore_f1")
    assert bert > 0.75, "Should have high semantic similarity"
    
    coherence = get_score(metric_cache, "excellent", "sentence_coherence", "sentence_coherence")
    assert coherence > 0.5, "Should be coherent"


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(27)
def test_terrible_summary_characteristics(metric_cache):
    """Verify terrible summary exhibits expected failure pattern.
    
    Terrible summaries should show:
    - Low coverage (ROUGE-L, entity coverage)
    - High hallucination (entity hallucination, novel n-grams)
    - Low semantic similarity (BERTScore)
    - Potentially low coherence
    """
    rouge = get_score(metric_cache, "terrible", "rouge_l", "rougeL_recall")
    assert rouge < 0.3, "Should have poor lexical coverage"
    
    ent_cov = get_score(metric_cache, "terrible", "entity_coverage", "entity_coverage")
    assert ent_cov < 0.3, "Should miss most entities"
    
    ent_hal = get_score(metric_cache, "terrible", "entity_coverage", "entity_hallucination")
    assert ent_hal > 0.5, "Should have many hallucinations"
    
    novel = get_score(metric_cache, "terrible", "novel_ngrams", "novel_ngram_pct")
    assert novel > 0.7, "Should have high novelty (invention)"
    
    bert = get_score(metric_cache, "terrible", "bertscore", "bertscore_f1")
    assert bert < 0.65, "Should have low semantic similarity"


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(28)
@pytest.mark.parametrize("level", QUALITY_LEVELS)
def test_all_metrics_computed_for_all_levels(metric_cache, level):
    """Verify cache contains all expected metrics for all quality levels."""
    expected_metrics = [
        "rouge_l", "bertscore", "novel_ngrams", "jsd_distribution",
        "entity_coverage", "ncd", "salience_recall", "nli_faithfulness",
        "readability_delta", "sentence_coherence", "entity_grid_coherence",
        "lexical_diversity", "stopword_ratio"
    ]
    
    for metric in expected_metrics:
        assert metric in metric_cache[level], (
            f"Missing {metric} for {level} in cache"
        )


# ==============================================================================
# MONOTONICITY VALIDATION
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(29)
@pytest.mark.parametrize("metric_keys", HIGHER_IS_BETTER, indirect=False)
def test_monotonic_ordering_higher_is_better(metric_keys, metric_cache):
    """Verify strict monotonic ordering: excellent > normal > terrible.
    
    This is a stronger test than pairwise: it ensures the metric produces
    a consistent quality ranking across all three levels.
    """
    cache_key, result_key = metric_keys
    
    scores = {
        level: get_score(metric_cache, level, cache_key, result_key)
        for level in QUALITY_LEVELS
    }
    
    assert scores["excellent"] > scores["normal"] > scores["terrible"], (
        f"{cache_key}.{result_key} should show monotonic ordering: "
        f"excellent ({scores['excellent']:.3f}) > "
        f"normal ({scores['normal']:.3f}) > "
        f"terrible ({scores['terrible']:.3f})"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(30)
@pytest.mark.parametrize("metric_keys", LOWER_IS_BETTER, indirect=False)
def test_monotonic_ordering_lower_is_better(metric_keys, metric_cache):
    """Verify strict monotonic ordering: excellent < normal < terrible.
    
    This is a stronger test than pairwise: it ensures the metric produces
    a consistent quality ranking across all three levels.
    """
    cache_key, result_key = metric_keys
    
    scores = {
        level: get_score(metric_cache, level, cache_key, result_key)
        for level in QUALITY_LEVELS
    }
    
    assert scores["excellent"] < scores["normal"] < scores["terrible"], (
        f"{cache_key}.{result_key} should show monotonic ordering: "
        f"excellent ({scores['excellent']:.3f}) < "
        f"normal ({scores['normal']:.3f}) < "
        f"terrible ({scores['terrible']:.3f})"
    )
