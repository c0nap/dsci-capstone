import pytest
from src.components.book_conversion import Chunk
from src.core.stages import *
from src.main import pipeline_B, pipeline_D
from conftest import optional_param
from typing import List


@pytest.fixture
def book_data():
    """Minimal data for smoke tests - mirrors book_2 structure"""
    chunk_id = "story-2_book-2_chapter-1_p.25000"

    chunk_path = f"./tests/examples-pipeline/chunks/{chunk_id}.txt"
    triples_path = f"./tests/examples-pipeline/triples/{chunk_id}.json"

    # Read chunk text and triples from files
    with open(chunk_path, 'r') as f:
        chunk_text = f.read()

    with open(triples_path, 'r') as f:
        llm_triples_json = f.read()

    chunk = Chunk(
        chunk_text,
        book_id=2,
        chapter_number=1,
        line_start=50,
        line_end=85,
        story_id=2,
        story_percent=8.0,
        chapter_percent=25.0,
    )

    return {
        "chunk": chunk,
        "chunk_text": chunk_text,
        "rebel_triples": [
            "children  had  carpet",
            "carpet  arrived  nursery",
            "egg  was_in  carpet",
            "egg  hatched  Phoenix",
            "Phoenix  can  talk",
            "Phoenix  is  ancient",
            "carpet  is  wishing_carpet",
            "carpet  takes_to  anywhere",
            "Phoenix  recommends  Egypt",
            "children  want  adventures",
        ],  # used to test LLM triple sanitization
        "llm_triples_json": llm_triples_json,
    }


@pytest.fixture
def rebel():
    """Fixture returning the REBEL extraction function."""
    return task_12_relation_extraction_rebel

@pytest.fixture
def openie():
    """Fixture returning the OpenIE extraction function."""
    return task_12_relation_extraction_openie

@pytest.fixture
def textacy():
    """Fixture returning the Textacy extraction function."""
    return task_12_relation_extraction_textacy

@pytest.fixture
def relation_extraction_task(request):
    """Meta-fixture that returns the backend function specified by the parameter."""
    return request.getfixturevalue(request.param)

PARAMS_RELATION_EXTRACTORS: List[pytest.param] = [
    optional_param("rebel", "transformers"),
    optional_param("openie", "stanza"),
    pytest.param("textacy"),     # test always runs (no dependency)
]


@pytest.fixture
def langchain():
    """Fixture returning the LangChain LLM API."""
    return task_14_relation_extraction_llm_langchain

@pytest.fixture
def openai():
    """Fixture returning the OpenAI LLM API."""
    return task_14_relation_extraction_llm_openai

@pytest.fixture
def llm_prompt_task(request):
    """Meta-fixture that returns the backend function specified by the parameter."""
    return request.getfixturevalue(request.param)


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.re
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="job_12_extraction_minimal", scope="session")
@pytest.mark.parametrize("relation_extractor", PARAMS_RELATION_EXTRACTORS, indirect=True)
def test_job_12_extraction_minimal(relation_extractor):
    """Parametrized test to verify all extractors return a standard list of tuples.
    @note Relying on default args ensures REBEL returns tuples (parse_tuples=True).
    """
    sample_text = "Alice met Bob in the forest. Bob then went to the village."
    extracted = relation_extractor(sample_text)
    assert isinstance(extracted, list)
    
    # If the model extracted anything, ensure it conforms to the standard (Subj, Rel, Obj) tuple
    if len(extracted) > 0:
        triple = extracted[0]
        assert isinstance(triple, tuple), f"Expected tuple output, got {type(triple)}"
        assert len(triple) == 3, "Tuple must have exactly 3 elements (Subj, Rel, Obj)"
        assert all(isinstance(x, str) for x in triple)


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.re
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="job_12_extraction_chunk", scope="session", depends=["job_12_extraction_minimal"])
@pytest.mark.parametrize("relation_extractor", PARAMS_RELATION_EXTRACTORS, indirect=True)
def test_job_12_extraction_chunk(book_data, relation_extractor):
    """Runs all extractors on realistic pipeline data in RAW mode.
    @details  Now validates that OpenIE/Textacy can produce stringified output consistent with REBEL.
    """
    # parse_tuples=False forces the class to return strings
    extracted = relation_extractor(book_data["chunk_text"], parse_tuples=False)

    assert isinstance(extracted, list)
    assert len(extracted) >= 5  # Realistic chunk should yield multiple triples
    assert all(isinstance(triple, str) for triple in extracted)
    
    # Verify the standardized delimiter is present (defined in Base Class)
    # This ensures downstream parsers don't break regardless of which model was used
    assert any("  " in triple for triple in extracted)


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.re
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="job_12_extraction_tuples", scope="session", depends=["job_12_extraction_chunk"])
@pytest.mark.parametrize("relation_extraction_task", PARAMS_RELATION_EXTRACTORS, indirect=True)
def test_job_12_extraction_tuples(book_data, relation_extractor):
    """Runs all extractors with tuple parsing on realistic data."""
    extracted = relation_extraction_task(book_data["chunk_text"], parse_tuples=True)

    assert isinstance(extracted, list)
    assert len(extracted) >= 5
    assert all(isinstance(triple, tuple) and len(triple) == 3 for triple in extracted)
    
    for subj, rel, obj in extracted:
        assert isinstance(subj, str) and len(subj) > 0
        assert isinstance(rel, str) and len(rel) > 0
        assert isinstance(obj, str) and len(obj) > 0


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.order(14)
@pytest.mark.dependency(name="job_14_llm_minimal", scope="session")
@pytest.mark.parametrize("llm_prompt_task", ["langchain", "openai"], indirect=True)
def test_job_14_llm_minimal(book_data):
    """Test LLM-based triple sanitization with realistic data."""
    triples_string = "\n".join(book_data["rebel_triples"])

    prompt, llm_output = llm_prompt_task(triples_string, book_data["chunk_text"])

    assert isinstance(prompt, str)
    assert triples_string in prompt
    assert book_data["chunk_text"] in prompt
    assert isinstance(llm_output, str)
    assert len(llm_output) > 0


@pytest.mark.pipeline
@pytest.mark.stage_B
@pytest.mark.smoke
@pytest.mark.order(120)
@pytest.mark.dependency(name="stage_B_minimal", scope="session", depends=["job_14_llm_minimal", "job_12_rebel_tuples"])
def test_pipeline_B_minimal(docs_db, book_data):
    """Test running the aggregate pipeline_B on smoke test data."""
    collection_name = "example_chunks"
    chunks = [book_data["chunk"]]
    book_title = "The Phoenix and the Carpet"

    triples, chunk = pipeline_B(collection_name, chunks, book_title)

    # Verify output structure
    assert isinstance(triples, list)
    assert len(triples) > 0
    assert isinstance(chunk, Chunk)

    # Verify each triple has required structure
    for triple in triples:
        assert "s" in triple
        assert "r" in triple
        assert "o" in triple

    # Verify chunk was inserted into MongoDB
    mongo_db = docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    doc = collection.find_one({"_id": chunk.get_chunk_id()})
    assert doc is not None
    assert doc["book_title"] == book_title


@pytest.mark.pipeline
@pytest.mark.stage_D
@pytest.mark.smoke
@pytest.mark.order(140)
@pytest.mark.dependency(name="stage_D_minimal", scope="session", depends=["pipeline_B_minimal"])
def test_pipeline_D_minimal(docs_db, book_data):
    """Test running pipeline_D with smoke test data."""
    collection_name = "test_pipeline_d_smoke"
    chunk = book_data["sample_chunk"]
    triples_string = "\n".join(book_data["rebel_triples"])

    # Insert chunk first - verified by pipeline_B_minimal
    task_11_send_chunk(chunk, collection_name, book_data["book_title"])

    summary = pipeline_D(collection_name, triples_string, chunk.get_chunk_id())

    assert isinstance(summary, str)
    assert len(summary) > 0

    # Verify summary was written to MongoDB
    mongo_db = docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    doc = collection.find_one({"_id": chunk.get_chunk_id()})
    assert doc is not None
    assert doc["summary"] == summary
