"""
Smoke tests for summary evaluation metrics.

Tests verify each metric runs successfully on realistic fiction text without errors.
These are not validation tests, they confirm the pipeline works.
"""

import importlib.util
from typing import Any, List
import pytest
from conftest import optional_param
from src.components.metrics import (
    run_rouge_l,
    run_bertscore,
    run_novel_ngrams,
    run_jsd_distribution,
    run_entity_coverage,
    run_ncd,
    run_salience_recall,
    run_nli_faithfulness,
    run_readability_delta,
    run_sentence_coherence,
    run_entity_grid_coherence,
    run_lexical_diversity,
    run_stopword_ratio,
)

@pytest.fixture
def book_data():
    """Realistic narrative fiction text for smoke testing."""
    return {
        "summary": (
            "The children discover a magical carpet with a Phoenix egg that hatches. "
            "They travel to exotic lands including India and a tropical island. "
            "The Phoenix helps them learn valuable lessons about responsibility and friendship. "
            "Eventually they must say goodbye when the Phoenix returns to its homeland."
        ),
        "text": (
            "It was a bright morning when Cyril, Robert, Anthea, and Jane found the dusty carpet "
            "rolled up in the attic. As they unrolled it, a strange egg tumbled out, gleaming golden "
            "in the dim light. 'What is it?' whispered Jane, her eyes wide with wonder. The egg began "
            "to crack, and out came a magnificent Phoenix, its feathers shimmering like flames. "
            "The Phoenix explained that the carpet was magical and could fly them anywhere in the world. "
            "Their first journey took them to a bustling Indian bazaar filled with spices and silk. "
            "The children marveled at snake charmers and jeweled elephants. On their second adventure, "
            "they flew to a deserted tropical island where they found treasure and learned to build shelter. "
            "The Phoenix taught them that magic comes with responsibility—they must keep their adventures "
            "secret and always return home by sunset. As weeks passed, the children grew attached to their "
            "feathered friend, but the Phoenix explained that it must return to its distant homeland to lay "
            "eggs of its own. With tears in their eyes, the children said farewell as the Phoenix flew into "
            "the sunset, promising to visit again someday. The carpet remained, a reminder of their magical summer."
        ),
    }


@pytest.fixture
def good_summary_data():
    """High-quality summary with good coverage and coherence."""
    return {
        "summary": (
            "Alice follows a White Rabbit down a rabbit hole into Wonderland, a strange world "
            "where she encounters peculiar characters. She attends a mad tea party with the "
            "March Hare and Hatter, meets the Cheshire Cat who can disappear, and plays croquet "
            "with the Queen of Hearts. After many adventures, Alice wakes to discover it was all a dream."
        ),
        "text": (
            "Alice was beginning to get very tired of sitting by her sister on the bank when suddenly "
            "a White Rabbit with pink eyes ran close by her. The Rabbit took a watch out of its pocket "
            "and hurried on. Alice followed it down a large rabbit hole beneath the hedge. She fell down "
            "what seemed to be a very deep well, landing in a hall with many locked doors. She found a "
            "key and a bottle labeled 'DRINK ME' which made her shrink. After many size changes, she "
            "entered a beautiful garden. There she met a Caterpillar smoking a hookah, and a Cheshire Cat "
            "that grinned and could vanish leaving only its smile. At a mad tea party, the March Hare, "
            "the Hatter, and the Dormouse told nonsensical stories. The Queen of Hearts invited her to "
            "play croquet using flamingoes as mallets and hedgehogs as balls. When the Queen ordered "
            "Alice's execution, Alice stood up to her, and suddenly found herself on the bank beside her "
            "sister. It had all been a curious dream."
        ),
    }


@pytest.fixture
def poor_summary_data():
    """Low-quality summary with hallucinations and missing key elements."""
    return {
        "summary": (
            "A girl named Sarah goes to a magical place and meets a wizard named Gandalf. "
            "They fight dragons together and find a treasure chest. The story ends happily."
        ),
        "text": (
            "Alice was beginning to get very tired of sitting by her sister on the bank when suddenly "
            "a White Rabbit with pink eyes ran close by her. The Rabbit took a watch out of its pocket "
            "and hurried on. Alice followed it down a large rabbit hole beneath the hedge. She fell down "
            "what seemed to be a very deep well, landing in a hall with many locked doors. She found a "
            "key and a bottle labeled 'DRINK ME' which made her shrink. After many size changes, she "
            "entered a beautiful garden. There she met a Caterpillar smoking a hookah, and a Cheshire Cat "
            "that grinned and could vanish leaving only its smile. At a mad tea party, the March Hare, "
            "the Hatter, and the Dormouse told nonsensical stories. The Queen of Hearts invited her to "
            "play croquet using flamingoes as mallets and hedgehogs as balls. When the Queen ordered "
            "Alice's execution, Alice stood up to her, and suddenly found herself on the bank beside her "
            "sister. It had all been a curious dream."
        ),
    }


# ==============================================================================
# METRIC FIXTURES
# ==============================================================================

@pytest.fixture
def rouge_l():
    """Fixture returning the ROUGE-L recall function."""
    return run_rouge_l


@pytest.fixture
def bertscore():
    """Fixture returning the BERTScore function."""
    return run_bertscore


@pytest.fixture
def novel_ngrams():
    """Fixture returning the novel n-grams function."""
    return run_novel_ngrams


@pytest.fixture
def jsd_distribution():
    """Fixture returning the JSD function."""
    return run_jsd_distribution


@pytest.fixture
def entity_coverage():
    """Fixture returning the entity coverage function."""
    return run_entity_coverage


@pytest.fixture
def ncd():
    """Fixture returning the NCD function."""
    return run_ncd


@pytest.fixture
def salience_recall():
    """Fixture returning the salience recall function."""
    return run_salience_recall


@pytest.fixture
def nli_faithfulness():
    """Fixture returning the NLI faithfulness function."""
    return run_nli_faithfulness


@pytest.fixture
def readability_delta():
    """Fixture returning the readability delta function."""
    return run_readability_delta


@pytest.fixture
def sentence_coherence():
    """Fixture returning the sentence coherence function."""
    return run_sentence_coherence


@pytest.fixture
def entity_grid_coherence():
    """Fixture returning the entity grid coherence function."""
    return run_entity_grid_coherence


@pytest.fixture
def lexical_diversity():
    """Fixture returning the lexical diversity function."""
    return run_lexical_diversity


@pytest.fixture
def stopword_ratio():
    """Fixture returning the stopword ratio function."""
    return run_stopword_ratio


@pytest.fixture
def evaluation_fn(request):
    """Meta-fixture that returns the evaluation function specified by the parameter."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def book_fixture(request):
    """Meta-fixture that returns the book data specified by the parameter."""
    return request.getfixturevalue(request.param)


# ==============================================================================
# PARAMETER LISTS
# ==============================================================================

# Metrics that compare summary to source and return values in [0, 1]
PARAMS_COMPARISON_METRICS_0_1: List[Any] = [
    optional_param("rouge_l", "rouge_score"),
    optional_param("bertscore", "evaluate"),
    optional_param("novel_ngrams", "nltk"),
    optional_param("ncd", "zlib"),
    optional_param("salience_recall", "sklearn"),
    optional_param("nli_faithfulness", "transformers"),
]

# Metrics that evaluate only the summary and return values in [0, 1]
PARAMS_REFERENCE_FREE_METRICS_0_1: List[Any] = [
    optional_param("sentence_coherence", "sentence_transformers"),
    optional_param("entity_grid_coherence", "spacy"),
    optional_param("lexical_diversity", "nltk"),
    optional_param("stopword_ratio", "nltk"),
]

# Entity coverage returns two metrics, both in [0, 1]
PARAMS_ENTITY_METRICS: List[Any] = [
    optional_param("entity_coverage", "spacy"),
]

# Metrics that don't necessarily return [0, 1] ranges
PARAMS_UNBOUNDED_METRICS: List[Any] = [
    optional_param("jsd_distribution", "scipy"),
    optional_param("readability_delta", "textstat"),
]

# All metrics for comprehensive testing
PARAMS_ALL_METRICS: List[Any] = (
    PARAMS_COMPARISON_METRICS_0_1
    + PARAMS_REFERENCE_FREE_METRICS_0_1
    + PARAMS_ENTITY_METRICS
    + PARAMS_UNBOUNDED_METRICS
)

# Book quality fixtures for comparative testing
PARAMS_BOOK_QUALITY: List[Any] = [
    pytest.param("good_summary_data"),
    pytest.param("poor_summary_data"),
]


# ==============================================================================
# PARAMETERIZED TESTS
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(1)
@pytest.mark.parametrize("evaluation_fn", PARAMS_COMPARISON_METRICS_0_1, indirect=True)
def test_comparison_metrics_0_1_range(evaluation_fn, book_data):
    """Verify comparison metrics run and return values in [0, 1]."""
    result = evaluation_fn(book_data["summary"], book_data["text"])
    
    # All metrics should return a dict with at least one key
    assert isinstance(result, dict)
    assert len(result) > 0
    
    # All values should be in [0, 1] range
    for key, value in result.items():
        assert 0 <= value <= 1, f"{key} = {value} not in [0, 1]"


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(2)
@pytest.mark.parametrize("evaluation_fn", PARAMS_REFERENCE_FREE_METRICS_0_1, indirect=True)
def test_reference_free_metrics_0_1_range(evaluation_fn, book_data):
    """Verify reference-free metrics run and return values in [0, 1]."""
    result = evaluation_fn(book_data["summary"])
    
    assert isinstance(result, dict)
    assert len(result) > 0
    
    for key, value in result.items():
        assert 0 <= value <= 1, f"{key} = {value} not in [0, 1]"


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(3)
@pytest.mark.parametrize("evaluation_fn", PARAMS_ENTITY_METRICS, indirect=True)
def test_entity_metrics_dual_output(evaluation_fn, book_data):
    """Verify entity metrics return both coverage and hallucination in [0, 1]."""
    result = evaluation_fn(book_data["summary"], book_data["text"])
    
    assert "entity_coverage" in result
    assert "entity_hallucination" in result
    assert 0 <= result["entity_coverage"] <= 1
    assert 0 <= result["entity_hallucination"] <= 1


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(4)
@pytest.mark.parametrize("evaluation_fn", PARAMS_UNBOUNDED_METRICS, indirect=True)
def test_unbounded_metrics_return_numbers(evaluation_fn, book_data):
    """Verify unbounded metrics run and return numeric values."""
    result = evaluation_fn(book_data["summary"], book_data["text"])
    
    assert isinstance(result, dict)
    assert len(result) > 0
    
    for key, value in result.items():
        assert isinstance(value, (int, float)), f"{key} = {value} is not numeric"


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(5)
def test_single_sentence_coherence_edge_case():
    """Verify coherence metrics handle single-sentence summaries gracefully."""
    single_sent = "The children found a magical carpet."
    
    coherence = run_sentence_coherence(single_sent)
    assert coherence["sentence_coherence"] == 1.0
    
    grid = run_entity_grid_coherence(single_sent)
    assert grid["entity_grid_coherence"] == 1.0


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(6)
def test_identical_texts_edge_case():
    """Verify metrics handle identical summary and source."""
    text = "The quick brown fox jumps over the lazy dog."
    
    rouge = run_rouge_l(text, text)
    assert rouge["rougeL_recall"] == 1.0
    
    novel = run_novel_ngrams(text, text)
    assert novel["novel_ngram_pct"] == 0.0


# ==============================================================================
# QUALITY COMPARISON TESTS
# ==============================================================================

@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(10)
@pytest.mark.parametrize("book_fixture", PARAMS_BOOK_QUALITY, indirect=True)
def test_quality_metrics_on_varying_summaries(book_fixture):
    """Verify metrics run on both good and poor quality summaries."""
    result = run_rouge_l(book_fixture["summary"], book_fixture["text"])
    assert "rougeL_recall" in result


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(11)
def test_good_summary_has_better_coverage(good_summary_data, poor_summary_data):
    """Verify good summary has higher ROUGE-L and entity coverage than poor summary."""
    good_rouge = run_rouge_l(good_summary_data["summary"], good_summary_data["text"])
    poor_rouge = run_rouge_l(poor_summary_data["summary"], poor_summary_data["text"])
    
    assert good_rouge["rougeL_recall"] > poor_rouge["rougeL_recall"], (
        "Good summary should have higher lexical coverage"
    )
    
    good_entities = run_entity_coverage(good_summary_data["summary"], good_summary_data["text"])
    poor_entities = run_entity_coverage(poor_summary_data["summary"], poor_summary_data["text"])
    
    assert good_entities["entity_coverage"] > poor_entities["entity_coverage"], (
        "Good summary should preserve more entities from source"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(12)
def test_poor_summary_has_more_hallucinations(good_summary_data, poor_summary_data):
    """Verify poor summary has higher entity hallucination rate."""
    good_entities = run_entity_coverage(good_summary_data["summary"], good_summary_data["text"])
    poor_entities = run_entity_coverage(poor_summary_data["summary"], poor_summary_data["text"])
    
    assert poor_entities["entity_hallucination"] > good_entities["entity_hallucination"], (
        "Poor summary should have more invented entities (Sarah, Gandalf, dragons)"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(13)
def test_poor_summary_has_higher_novelty(good_summary_data, poor_summary_data):
    """Verify poor summary has higher novel n-gram percentage."""
    good_novel = run_novel_ngrams(good_summary_data["summary"], good_summary_data["text"])
    poor_novel = run_novel_ngrams(poor_summary_data["summary"], poor_summary_data["text"])
    
    assert poor_novel["novel_ngram_pct"] > good_novel["novel_ngram_pct"], (
        "Poor summary should have more novel n-grams due to hallucinated content"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(14)
def test_good_summary_has_better_semantic_similarity(good_summary_data, poor_summary_data):
    """Verify good summary has higher BERTScore than poor summary."""
    good_bert = run_bertscore(good_summary_data["summary"], good_summary_data["text"])
    poor_bert = run_bertscore(poor_summary_data["summary"], poor_summary_data["text"])
    
    assert good_bert["bertscore_f1"] > poor_bert["bertscore_f1"], (
        "Good summary should have higher semantic similarity to source"
    )


@pytest.mark.smoke
@pytest.mark.eval
@pytest.mark.order(15)
def test_good_summary_has_better_coherence(good_summary_data, poor_summary_data):
    """Verify good summary has higher sentence coherence."""
    good_coherence = run_sentence_coherence(good_summary_data["summary"])
    poor_coherence = run_sentence_coherence(poor_summary_data["summary"])
    
    assert good_coherence["sentence_coherence"] > poor_coherence["sentence_coherence"], (
        "Good summary should have smoother transitions between sentences"
    )
