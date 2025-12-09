from tests.helpers import optional_param
from tests.conftest import *
import pytest
from src.components.book_conversion import Chunk
from src.core.stages import *
from src.main import pipeline_B, pipeline_D, pipeline_E
from typing import Any, List
from src.core.stages import Config


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
        "book_id": 1,
        "book_title": "Five Children and It",
        "summary": "The children discover a magical carpet with a Phoenix.",
        "gold_summary": "Children find magical carpet.",
        "chunk": chunk,
        "json_triples" : [
            {"s": "children", "r": "had", "o": "carpet"},
            {"s": "carpet", "r": "arrived", "o": "nursery"},
            {"s": "egg", "r": "was_in", "o": "carpet"},
            {"s": "egg", "r": "hatched", "o": "Phoenix"},
            {"s": "Phoenix", "r": "can", "o": "talk"},
            {"s": "Phoenix", "r": "is", "o": "ancient"},
            {"s": "carpet", "r": "is", "o": "wishing_carpet"},
            {"s": "carpet", "r": "takes_to", "o": "anywhere"},
            {"s": "Phoenix", "r": "recommends", "o": "Egypt"},
            {"s": "children", "r": "want", "o": "adventures"},
        ],
        "llm_triples_json": llm_triples_json,
        "bookscore": 0.85,
        "questeval": 0.92,
    }


@pytest.fixture
def rebel(monkeypatch):
    """Fixture returning the REBEL extraction function."""
    monkeypatch.setattr(Config, "relation_extractor_type", "rebel")


@pytest.fixture
def openie(monkeypatch):
    """Fixture returning the OpenIE extraction function."""
    monkeypatch.setattr(Config, "relation_extractor_type", "openie")


@pytest.fixture
def textacy(monkeypatch):
    """Fixture returning the Textacy extraction function."""
    monkeypatch.setattr(Config, "relation_extractor_type", "textacy")


@pytest.fixture
def extractor_type(request):
    """Meta-fixture that returns the backend function specified by the parameter."""
    return request.getfixturevalue(request.param)


PARAMS_RELATION_EXTRACTORS: List[Any] = [  # ParameterSet is internal to PyTest
    optional_param("rebel", "transformers"),
    optional_param("openie", "stanza"),
    pytest.param("textacy"),  # test always runs (no dependency)
]


@pytest.fixture
def langchain(monkeypatch):
    """Fixture returning the LangChain LLM API."""
    monkeypatch.setattr(Config, "validation_llm_engine", "langchain")


@pytest.fixture
def openai(monkeypatch):
    """Fixture returning the OpenAI LLM API."""
    monkeypatch.setattr(Config, "validation_llm_engine", "openai")


@pytest.fixture
def llm_connector_type(request):
    """Meta-fixture that returns the backend function specified by the parameter."""
    return request.getfixturevalue(request.param)


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.re
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="job_12_extraction_minimal", scope="session")
@pytest.mark.parametrize("extractor_type", PARAMS_RELATION_EXTRACTORS, indirect=True)
def test_job_12_extraction_minimal(extractor_type):
    """Parametrized test to verify all extractors return a standard list of Triple dicts.
    @note Relying on default args ensures REBEL parses output to Triples.
    """
    sample_text = "Alice met Bob in the forest. Bob then went to the village."
    extracted = task_12_relation_extraction(sample_text)
    assert isinstance(extracted, list)

    # If the model extracted anything, ensure it conforms to the standard Triple dict
    if len(extracted) > 0:
        triple = extracted[0]
        assert isinstance(triple, dict), f"Expected dict output, got {type(triple)}"
        assert all(key in triple for key in ["s", "r", "o"]), "Triple must have keys 's', 'r', and 'o'"
        assert all(isinstance(val, str) for val in triple.values())


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.re
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="job_12_extraction_chunk", scope="session", depends=["job_12_extraction_minimal"])
@pytest.mark.parametrize("extractor_type", PARAMS_RELATION_EXTRACTORS, indirect=True)
def test_job_12_extraction(book_data, extractor_type):
    """Runs all extractors on realistic pipeline data."""
    extracted = task_12_relation_extraction(book_data["chunk"].text)

    assert isinstance(extracted, list)
    # Flexible check: we expect some results, but exact count depends on the model
    assert len(extracted) >= 1
    assert all(isinstance(t, dict) for t in extracted)

    for triple in extracted:
        subj = triple["s"]
        rel = triple["r"]
        obj = triple["o"]
        assert isinstance(subj, str) and len(subj) > 0
        assert isinstance(rel, str) and len(rel) > 0
        assert isinstance(obj, str) and len(obj) > 0


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.llm
@pytest.mark.smoke
@pytest.mark.order(14)
@pytest.mark.dependency(name="job_14_llm_minimal", scope="session")
@pytest.mark.parametrize("llm_connector_type", ["langchain", "openai"], indirect=True)
def test_job_14_llm_minimal(book_data, llm_connector_type):
    """Test LLM-based triple sanitization with realistic data."""
    triples = book_data["json_triples"]
    prompt, llm_output, _ = task_14_validate_llm(triples, book_data["chunk"].text)

    assert isinstance(prompt, str)
    assert str(triples) in prompt
    assert book_data["chunk"].text in prompt
    assert isinstance(llm_output, str)
    assert len(llm_output) > 0


@pytest.mark.pipeline
@pytest.mark.stage_B
@pytest.mark.smoke
@pytest.mark.order(120)
@pytest.mark.dependency(name="stage_B_minimal", scope="session", depends=["job_14_llm_minimal", "job_12_extraction_chunk"])
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
@pytest.mark.dependency(name="stage_D_minimal", scope="session", depends=["stage_B_minimal"])
def test_pipeline_D_minimal(docs_db, book_data):
    """Test running pipeline_D with smoke test data."""
    collection_name = "test_pipeline_d_smoke"
    chunk = book_data["chunk"]
    triples_string = "\n".join(book_data["json_triples"])

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


@pytest.mark.pipeline
@pytest.mark.stage_E
@pytest.mark.smoke
@pytest.mark.order(150)
@pytest.mark.dependency(name="stage_E_minimal", scope="session")
def test_pipeline_E_minimal_summary_only(book_data):
    """Test running pipeline_E with summary-only mode.
    @note  Requires Blazor to accept POST."""
    summary = book_data["summary"]
    book_title = book_data["book_title"]
    book_id = str(book_data["book_id"])

    # TODO: Cannot verify output - need task_40_post_payload implementation

    # Test summary-only path (no chunk parameter)
    pipeline_E(summary, book_title, book_id)

    assert True  # Placeholder - verifies no exceptions raised


@pytest.mark.pipeline
@pytest.mark.stage_E
@pytest.mark.smoke
@pytest.mark.order(151)
@pytest.mark.dependency(name="stage_E_payload", scope="session", depends=["stage_E_minimal"])
def test_pipeline_E_minimal_full_payload(book_data):
    """Test running pipeline_E with full payload including metrics.
    @note  Requires Blazor to accept POST."""
    summary = book_data["summary"]
    book_title = book_data["book_title"]
    book_id = str(book_data["book_id"])
    chunk_text = book_data["chunk"].text
    gold_summary = book_data["gold_summary"]
    bookscore = book_data["bookscore"]
    questeval = book_data["questeval"]

    # TODO: Cannot verify output - need task_40_post_payload implementation

    # Test full payload path
    pipeline_E(summary, book_title, book_id, chunk_text, gold_summary, bookscore, questeval)

    assert True  # Placeholder - verifies no exceptions raised
