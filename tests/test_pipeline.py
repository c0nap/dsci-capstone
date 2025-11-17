import pytest
from src.core.stages import (
    linear_01_convert_epub,
    linear_02_parse_chapters,
    linear_03_chunk_story,
    pipeline_A,
)
from src.components.book_conversion import EPUBToTEI, ParagraphStreamTEI, Story
import os
from pandas import read_csv

##########################################################################
# Fixtures
##########################################################################

@pytest.fixture
def book_data(request):
    return request.getfixturevalue(request.param)

@pytest.fixture
def book_1_data():
    """Example data for Book 1: nested-fairy-tales.epub"""
    return {
        "epub": "./datasets/examples/nested-fairy-tales.epub",
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
    }

@pytest.fixture
def book_2_data():
    """Example data for Book 2: nested-myths.epub"""
    return {
        "epub": "./datasets/examples/nested-myths.epub",
        "chapters": """
            CHAPTER 1 ORIGINS
            CHAPTER 2 HEROES
            CHAPTER 3 GODS AND MONSTERS
            CHAPTER 4 TRIALS
            CHAPTER 5 RESOLUTIONS
        """,
        "start": "",
        "end": "Thus ends the tale.",
        "book_id": 2,
        "story_id": 1,
    }

##########################################################################
# Tests
##########################################################################

@pytest.mark.pipeline
@pytest.mark.order(1)
@pytest.mark.dependency(name="linear_01", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_linear_01_convert_epub(book_data):
    """Test EPUB -> TEI conversion for multiple books."""
    tei_path = linear_01_convert_epub(book_data["epub"])
    assert tei_path.endswith(".tei")
    assert os.path.exists(tei_path)


@pytest.mark.pipeline
@pytest.mark.order(2)
@pytest.mark.dependency(name="linear_02", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_linear_02_parse_chapters(book_data):
    """Test TEI -> Story parsing for multiple books."""
    tei_path = linear_01_convert_epub(book_data["epub"])
    story = linear_02_parse_chapters(
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
@pytest.mark.dependency(name="linear_03", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_linear_03_chunk_story(book_data):
    """Test Story -> chunks splitting for multiple books."""
    tei_path = linear_01_convert_epub(book_data["epub"])
    story = linear_02_parse_chapters(
        tei_path,
        book_data["chapters"],
        book_data["book_id"],
        book_data["story_id"],
        book_data["start"],
        book_data["end"],
    )
    chunks = linear_03_chunk_story(story)
    assert isinstance(chunks, list)
    assert len(chunks) > 0
    for c in chunks:
        assert hasattr(c, "text")
        assert len(c.text) > 0




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
    """Read example CSV and run pipeline_A for each row."""
    csv_path = "datasets/books.csv"
    assert os.path.exists(csv_path)

    df = read_csv(csv_path)
    for _, row in df.iterrows():
        epub_path = row.get("epub_path")
        start_str = row.get("start_string") or None
        end_str = row.get("end_string") or None
        chapters = row.get("chapters", "")
        book_id = row.get("book_id")
        story_id = int(row.get("story_id"))

        chunks = pipeline_A(epub_path, chapters, start_str, end_str, book_id, story_id)
        assert isinstance(chunks, list)
        assert len(chunks) > 0

