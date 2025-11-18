import pytest
from src.core.stages import *
from src.main import pipeline_A
from src.components.book_conversion import EPUBToTEI, ParagraphStreamTEI, Story, Chunk
import os
from pandas import read_csv
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
        llm_triples_json = json.load(f)

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
    }


##########################################################################
# Tests
##########################################################################

@pytest.mark.pipeline
@pytest.mark.order(1)
@pytest.mark.dependency(name="task_01", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_01_convert_epub(book_data):
    """Test EPUB -> TEI conversion for multiple books."""
    tei_path = task_01_convert_epub(book_data["epub"])
    assert tei_path.endswith(".tei")
    assert os.path.exists(tei_path)


@pytest.mark.pipeline
@pytest.mark.order(2)
@pytest.mark.dependency(name="task_02", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_02_parse_chapters(book_data):
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


@pytest.mark.pipeline
@pytest.mark.order(3)
@pytest.mark.dependency(name="task_03", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_03_chunk_story(book_data):
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


@pytest.mark.pipeline
@pytest.mark.order(10)
@pytest.mark.dependency(name="task_10_sample", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_10_sample_chunks(book_data):
    """Test sampling multiple chunks from a list."""
    chunks = book_data["chunks_list"]
    n_sample = 2
    
    unique_numbers, sample = task_10_sample_chunks(chunks, n_sample)
    
    assert len(unique_numbers) == n_sample
    assert len(sample) == n_sample
    assert all(0 <= idx < len(chunks) for idx in unique_numbers)
    assert all(isinstance(c, Chunk) for c in sample)


@pytest.mark.pipeline
@pytest.mark.order(10)
@pytest.mark.dependency(name="task_10_random", scope="session", depends=["task_10_sample"])
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_10_random_chunk(book_data):
    """Test selecting a single random chunk."""
    chunks = book_data["chunks_list"]
    
    unique_number, chunk = task_10_random_chunk(chunks)
    
    assert isinstance(unique_number, int)
    assert 0 <= unique_number < len(chunks)
    assert isinstance(chunk, Chunk)


@pytest.mark.pipeline
@pytest.mark.order(11)
@pytest.mark.dependency(name="task_11", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_11_send_chunk(book_data):
    """Test inserting chunk into MongoDB collection."""
    chunk = book_data["sample_chunk"]
    collection_name = "pytest"
    book_title = book_data["book_title"]
    
    task_11_send_chunk(chunk, collection_name, book_title)
    
    # Verify chunk was inserted with correct book_title
    mongo_db = docs_db.get_unmanaged_handle()
    collection = getattr(mongo_db, collection_name)
    doc = collection.find_one({"_id": chunk.get_chunk_id()})
    
    assert doc is not None
    assert doc["book_title"] == book_title
    assert doc["text"] == chunk.text


@pytest.mark.pipeline
@pytest.mark.order(13)
@pytest.mark.dependency(name="task_13", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_13_concatenate_triples(book_data):
    """Test converting extracted triples to newline-delimited string."""
    extracted = book_data["rebel_triples"]
    
    triples_string = task_13_concatenate_triples(extracted)
    
    assert isinstance(triples_string, str)
    assert triples_string.count("\n") == len(extracted)
    # Verify each triple appears in output
    for triple in extracted:
        assert str(triple) in triples_string


@pytest.mark.pipeline
@pytest.mark.order(15)
@pytest.mark.dependency(name="task_15", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_15_sanitize_triples_llm(book_data):
    """Test parsing LLM output JSON into triples list."""
    llm_output = book_data["llm_triples_json"]
    
    triples = task_15_sanitize_triples_llm(llm_output)
    
    assert isinstance(triples, list)
    assert len(triples) > 0
    # Verify structure: each triple should have s, r, o keys
    for triple in triples:
        assert "s" in triple
        assert "r" in triple
        assert "o" in triple






##########################################################################
# Minimal aggregate test
##########################################################################

@pytest.mark.pipeline
@pytest.mark.order(4)
@pytest.mark.dependency(name="pipeline_A_minimal", scope="session")
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
@pytest.mark.order(5)
@pytest.mark.dependency(name="pipeline_A_csv", scope="session")
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

