import pytest
from src.core.stages import *
from src.main import pipeline_A, pipeline_C, pipeline_E
from src.components.book_conversion import EPUBToTEI, ParagraphStreamTEI, Story, Chunk
import os
from pandas import DataFrame, read_csv
from src.util import Log

##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def book_data(request):
    return request.getfixturevalue(request.param)

@pytest.fixture
def book_1_data():
    """Example data for Book 1: Five Children and It"""
    sample_chunk = Chunk(
        "The Psammead granted wishes.",
        book_id=1,
        chapter_number=2,
        line_start=20,
        line_end=22,
        story_id=1,
        story_percent=10.0,
        chapter_percent=5.0,
    )

    return {
        "epub": "./tests/examples-pipeline/epub/trilogy-wishes-1.epub",
        "chapters": """
            CHAPTER 1 BEAUTIFUL AS THE DAY
            CHAPTER 2 GOLDEN GUINEAS
            CHAPTER 3 BEING WANTED
            CHAPTER 4 WINGS
            CHAPTER 5 NO WINGS
            CHAPTER 6 A CASTLE AND NO DINNER
            CHAPTER 7 A SIEGE AND BED
            CHAPTER 8 BIGGER THAN THE BAKER'S BOY
            CHAPTER 9 GROWN UP
            CHAPTER 10 SCALPS
            CHAPTER 11 THE LAST WISH
        """,
        "start": "",
        "end": "But I must say no more.",
        "book_id": 1,
        "story_id": 1,
        "book_title": "Five Children and It",
        "tei": "./tests/examples-pipeline/epub/trilogy-wishes-1.tei",
        "chunks_list": [
            Chunk("The children were digging.", 1, 1, 10, 11, 1, 5.0, 10.0),
            Chunk("They found a Psammead.", 1, 1, 12, 13, 1, 5.5, 15.0),
            Chunk("It was very grumpy.", 1, 1, 14, 15, 1, 6.0, 20.0),
            sample_chunk,
            Chunk("The children wished to be beautiful.", 1, 2, 22, 23, 1, 10.5, 10.0),
            Chunk("Everyone stared at them.", 1, 2, 24, 25, 1, 11.0, 15.0),
            Chunk("They wished for gold.", 1, 3, 30, 31, 1, 15.0, 5.0),
            Chunk("The gold caused problems.", 1, 3, 32, 33, 1, 15.5, 10.0),
            Chunk("Wishes always go wrong.", 1, 3, 34, 35, 1, 16.0, 15.0),
            Chunk("They learned their lesson.", 1, 3, 36, 37, 1, 16.5, 20.0),
        ],
        "sample_chunk": sample_chunk,
        "rebel_triples": ["children  found  Psammead", "Psammead  grants  wishes"],
        "llm_triples_json": '[{"s": "children", "r": "found", "o": "Psammead"}, {"s": "Psammead", "r": "grants", "o": "wishes"}]',
        "num_triples": 2
    }

@pytest.fixture
def book_2_data():
    """Example data for Book 2: The Phoenix and the Carpet - realistic pipeline data"""
    chunk_1_id = "story-2_book-2_chapter-1_p.25000"
    chunk_2_id = "story-2_book-2_chapter-1_p.50000"
    
    chunk_1_path = f"./tests/examples-pipeline/chunks/{chunk_1_id}.txt"
    chunk_2_path = f"./tests/examples-pipeline/chunks/{chunk_2_id}.txt"
    triples_1_path = f"./tests/examples-pipeline/triples/{chunk_1_id}.json"
    
    # Read chunk texts from files
    with open(chunk_1_path, 'r') as f:
        chunk_1_text = f.read()
    with open(chunk_2_path, 'r') as f:
        chunk_2_text = f.read()
    
    # Read triples from files
    with open(triples_1_path, 'r') as f:
        llm_triples_json = f.read()

    chunk_1 = Chunk(
        chunk_1_text,
        book_id=2,
        chapter_number=1,
        line_start=50,
        line_end=85,
        story_id=2,
        story_percent=8.0,
        chapter_percent=25.0,
    )
    chunk_2 = Chunk(
        chunk_2_text,
        book_id=2,
        chapter_number=1,
        line_start=86,
        line_end=120,
        story_id=2,
        story_percent=12.0,
        chapter_percent=50.0,
    )

    return {
        "epub": "./tests/examples-pipeline/epub/trilogy-wishes-2.epub",
        "chapters": """
            CHAPTER 1. THE EGG\n
            CHAPTER 2. THE TOPLESS TOWER\n
            CHAPTER 3. THE QUEEN COOK\n
            CHAPTER 4. TWO BAZAARS\n
            CHAPTER 5. THE TEMPLE\n
            CHAPTER 6. DOING GOOD\n
            CHAPTER 7. MEWS FROM PERSIA\n
            CHAPTER 8. THE CATS, THE COW, AND THE BURGLAR\n
            CHAPTER 9. THE BURGLAR’S BRIDE\n
            CHAPTER 10. THE HOLE IN THE CARPET\n
            CHAPTER 11. THE BEGINNING OF THE END\n
            CHAPTER 12. THE END OF THE END\n
        """,
        "start": "",
        "end": "end of the Phoenix and the Carpet.",
        "book_id": 2,
        "story_id": 1,
        "book_title": "The Phoenix and the Carpet",
        "tei": "./tests/examples-pipeline/epub/trilogy-wishes-2.tei",
        "chunks_list": [
            chunk_1,
            chunk_2,
        ],
        "sample_chunk": chunk_1,
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
        ],
        "llm_triples_json": llm_triples_json,
        "num_triples": 10,
    }


##########################################################################
# Tests
##########################################################################

@pytest.mark.task
@pytest.mark.stage_A
@pytest.mark.order(1)
@pytest.mark.dependency(name="job_01", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_01_convert_epub(book_data):
    """Test EPUB -> TEI conversion for multiple books."""
    tei_path = task_01_convert_epub(book_data["epub"])
    assert tei_path.endswith(".tei")
    assert os.path.exists(tei_path)


@pytest.mark.task
@pytest.mark.stage_A
@pytest.mark.order(2)
@pytest.mark.dependency(name="job_02", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_02_parse_chapters(book_data):
    """Test TEI -> Story parsing for multiple books."""
    # TODO - save data as fixture instead of calling earlier?
    tei_path = book_data["tei"]
    story = task_02_parse_chapters(
        tei_path,
        book_data["chapters"],
        book_data["book_id"],
        book_data["story_id"],
        book_data["start"],
        book_data["end"],
    )
    assert isinstance(story, Story)
    assert hasattr(story, "reader")


@pytest.mark.task
@pytest.mark.stage_A
@pytest.mark.order(3)
@pytest.mark.dependency(name="job_03", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_03_chunk_story(book_data):
    """Test Story -> chunks splitting for multiple books."""
    tei_path = book_data["tei"]
    # TODO - save data as fixture instead of calling earlier?
    story = task_02_parse_chapters(
        tei_path,
        book_data["chapters"],
        book_data["book_id"],
        book_data["story_id"],
        book_data["start"],
        book_data["end"],
    )
    chunks = task_03_chunk_story(story)
    assert isinstance(chunks, list)
    assert len(chunks) > 0
    for c in chunks:
        assert hasattr(c, "text")
        assert len(c.text) > 0


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.order(10)
@pytest.mark.dependency(name="job_10_multi", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_10_sample_chunks(book_data):
    """Test sampling multiple chunks from a list."""
    chunks = book_data["chunks_list"]
    n_sample = 2
    
    unique_numbers, sample = task_10_sample_chunks(chunks, n_sample)
    
    assert len(unique_numbers) == n_sample
    assert len(sample) == n_sample
    assert all(0 <= idx < len(chunks) for idx in unique_numbers)
    assert all(isinstance(c, Chunk) for c in sample)


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.order(10)
@pytest.mark.dependency(name="job_10_single", scope="session", depends=["job_10_multi"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_10_random_chunk(book_data):
    """Test selecting a single random chunk."""
    chunks = book_data["chunks_list"]
    
    unique_number, chunk = task_10_random_chunk(chunks)
    
    assert isinstance(unique_number, int)
    assert 0 <= unique_number < len(chunks)
    assert isinstance(chunk, Chunk)


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.order(11)
@pytest.mark.dependency(name="job_11", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_11_send_chunk(docs_db, book_data):
    """Test inserting chunk into MongoDB collection."""
    chunk = book_data["sample_chunk"]
    collection_name = "example_chunks"
    book_title = book_data["book_title"]
    
    task_11_send_chunk(chunk, collection_name, book_title)
    
    # Verify chunk was inserted with correct book_title
    mongo_db = docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    doc = collection.find_one({"_id": chunk.get_chunk_id()})
    
    assert doc is not None
    assert doc["book_title"] == book_title
    assert doc["text"] == chunk.text


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.order(13)
@pytest.mark.dependency(name="job_13", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_13_concatenate_triples(book_data):
    """Test converting extracted triples to newline-delimited string."""
    extracted = book_data["rebel_triples"]
    
    triples_string = task_13_concatenate_triples(extracted)
    
    assert isinstance(triples_string, str)
    assert triples_string.count("\n") == len(extracted)
    # Verify each triple appears in output
    for triple in extracted:
        assert str(triple) in triples_string


@pytest.mark.task
@pytest.mark.stage_B
@pytest.mark.order(15)
@pytest.mark.dependency(name="job_15_minimal", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_15_sanitize_triples_llm(book_data):
    """Test parsing LLM output JSON into triples list."""
    llm_output = book_data["llm_triples_json"]
    num_triples = book_data["num_triples"]
    
    triples = task_15_sanitize_triples_llm(llm_output)
    
    assert isinstance(triples, list)
    assert len(triples) == num_triples  # Matches fixture data
    # Verify structure: each triple has s, r, o keys
    for triple in triples:
        assert "s" in triple
        assert "r" in triple
        assert "o" in triple
        assert isinstance(triple["s"], str) and len(triple["s"]) > 0
        assert isinstance(triple["r"], str) and len(triple["r"]) > 0
        assert isinstance(triple["o"], str) and len(triple["o"]) > 0


# @pytest.mark.task
# @pytest.mark.stage_B
# @pytest.mark.order(15)
# @pytest.mark.dependency(name="task_15_comprehensive", scope="session", depends=["job_15_minimal"])
# def test_job_15_comprehensive(book_data):
#     """Test parsing realistic LLM JSON output."""
#     triples = task_15_sanitize_triples_llm(book_data["llm_triples_json"])
    
#     # TODO - normalize_triples with malformed llm output
#     pass



@pytest.mark.task
@pytest.mark.stage_C
@pytest.mark.order(20)
@pytest.mark.dependency(name="job_20", scope="session", depends=["job_15_minimal"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_20_send_triples(main_graph, book_data):
    """Test inserting triples into knowledge graph."""
    triples_json = task_15_sanitize_triples_llm(book_data["llm_triples_json"])
    
    task_20_send_triples(triples_json)
    
    # Verify triples were inserted
    triples_df = main_graph.get_all_triples()
    assert "subject_id" in triples_df.columns
    assert "relation_id" in triples_df.columns
    assert "object_id" in triples_df.columns
    assert len(triples_df) > 0


@pytest.mark.task
@pytest.mark.stage_C
@pytest.mark.order(21)
@pytest.mark.dependency(name="job_21", scope="session", depends=["job_20", "job_15_minimal"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_21_describe_graph(main_graph, book_data):
    """Test generating edge count summary of knowledge graph."""
    triples_json = task_15_sanitize_triples_llm(book_data["llm_triples_json"])

    # First insert triples, treat task_20 as a helper function
    # This is safe because we use function-scoped fixtures (data is dropped) and depend on task_11 passing.
    task_20_send_triples(triples_json)
    
    edge_count_df = group_21_1_describe_graph()
    
    assert isinstance(edge_count_df, DataFrame)
    assert "node_name" in edge_count_df.columns
    assert "edge_count" in edge_count_df.columns
    assert len(edge_count_df) > 0


@pytest.mark.task
@pytest.mark.stage_C
@pytest.mark.order(22)
@pytest.mark.dependency(name="job_22", scope="session", depends=["job_20", "job_15_minimal"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_22_verbalize_triples(main_graph, book_data):
    """Test converting high-degree triples to string format."""
    triples_json = task_15_sanitize_triples_llm(book_data["llm_triples_json"])
    # First insert triples, treat task_20 as a helper function
    # This is safe because we use function-scoped fixtures (data is dropped) and depend on task_11 passing.
    task_20_send_triples(triples_json)
    
    triples_string = task_22_verbalize_triples()
    
    assert isinstance(triples_string, str)
    assert len(triples_string) > 0


@pytest.mark.task
@pytest.mark.stage_D
@pytest.mark.order(31)
@pytest.mark.dependency(name="job_31", scope="session", depends=["job_11"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_job_31_send_summary(docs_db, book_data):
    """Test updating chunk with summary in MongoDB."""
    chunk = book_data["sample_chunk"]
    collection_name = "example_chunks"
    summary = "The children discover a magical carpet with a Phoenix egg inside."
    
    # First insert the chunk, treat task_11 as a helper function now
    # This is safe because we use function-scoped fixtures (data is dropped) and depend on task_11 passing.
    task_11_send_chunk(chunk, collection_name, book_data["book_title"])
    
    # Then add summary
    task_31_send_summary(summary, collection_name, chunk.get_chunk_id())
    
    # Verify summary was added
    mongo_db = docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    doc = collection.find_one({"_id": chunk.get_chunk_id()})
    
    assert doc is not None
    assert doc["summary"] == summary



##########################################################################
# Minimal aggregate test
##########################################################################

@pytest.mark.pipeline
@pytest.mark.stage_A
@pytest.mark.order(4)
@pytest.mark.dependency(name="stage_A_minimal", scope="session", depends=["job_03", "job_02", "job_01"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_pipeline_A_minimal(book_data):
    """Test running the aggregate pipeline_A on a single book."""
    data = book_data
    chunks = pipeline_A(
        data["epub"],
        data["chapters"],
        data["start"],
        data["end"],
        data["book_id"],
        data["story_id"],
    )
    assert isinstance(chunks, list)
    assert len(chunks) > 0


@pytest.mark.pipeline
@pytest.mark.stage_A
@pytest.mark.order(110)
@pytest.mark.dependency(name="stage_A_csv", scope="session")
def test_pipeline_A_from_csv():
    """Read example CSV and run pipeline_A for each row.
    @details
    - Excel -> Save As -> CSV (UTF-8)
    - Pandas will convert all blanks to None, so we must undo using fillna."""
    csv_path = "./tests/examples-pipeline/books.csv"
    assert os.path.exists(csv_path)

    df = read_csv(csv_path).fillna("")  # necessary for start_string blank
    for _, row in df.iterrows():
        epub_path = row["epub_path"]
        start_str = row["start_string"]
        end_str = row["end_string"]
        chapters = row["chapters"]
        book_id = row["book_id"]
        story_id = int(row["story_id"])

        chunks = pipeline_A(epub_path, chapters, start_str, end_str, book_id, story_id)
        assert isinstance(chunks, list)
        assert len(chunks) > 0


@pytest.mark.pipeline
@pytest.mark.stage_C
@pytest.mark.order(130)
@pytest.mark.dependency(name="stage_C_minimal", scope="session", depends=["job_20", "job_22"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_pipeline_C_minimal(main_graph, book_data):
    """Test running pipeline_C with smoke test data."""
    json_triples = json.loads(book_data["llm_triples_json"])
    
    triples_string = pipeline_C(json_triples)
    
    # Verify triples were inserted
    triples_df = main_graph.get_all_triples()
    assert "subject_id" in triples_df.columns
    assert "relation_id" in triples_df.columns
    assert "object_id" in triples_df.columns
    assert len(triples_df) > 0

    assert isinstance(triples_string, str)
    assert len(triples_string) > 0


@pytest.mark.pipeline
@pytest.mark.stage_E
@pytest.mark.order(150)
@pytest.mark.dependency(name="stage_E_minimal", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_pipeline_E_minimal_summary_only(book_data):
    """Test running pipeline_E with summary-only mode."""
    summary = "The children discover a magical carpet with a Phoenix."
    book_title = book_data["book_title"]
    book_id = str(book_data["book_id"])
    
    # Test summary-only path (no chunk parameter)
    pipeline_E(summary, book_title, book_id)
    
    # TODO: Cannot verify without task_40_post_summary implementation
    assert True  # Placeholder - verifies no exceptions raised


@pytest.mark.pipeline
@pytest.mark.stage_E
@pytest.mark.order(151)
@pytest.mark.dependency(name="stage_E_payload", scope="session", depends=["stage_E_minimal"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_pipeline_E_minimal_full_payload(book_data):
    """Test running pipeline_E with full payload including metrics."""
    summary = "The children discover a magical carpet with a Phoenix."
    book_title = book_data["book_title"]
    book_id = str(book_data["book_id"])
    chunk_text = book_data["sample_chunk"].text
    gold_summary = "Children find magical carpet."
    bookscore = 0.85
    questeval = 0.92
    
    # Test full payload path
    pipeline_E(summary, book_title, book_id, chunk_text, gold_summary, bookscore, questeval)
    
    # TODO: Cannot verify without task_40_post_payload implementation
    assert True  # Placeholder - verifies no exceptions raised