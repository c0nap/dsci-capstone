import pytest
from src.core.stages import (
    linear_01_convert_epub,
    linear_02_parse_chapters,
    linear_03_chunk_story,
    pipeline_A,
)
from src.components.epub_to_tei import EPUBToTEI
from src.core.story import ParagraphStreamTEI, Story
import os

@pytest.fixture
def example_data():
    """Returns a dictionary of all example data for tests."""
    return {
        "epub_1": "./datasets/examples/nested-fairy-tales.epub",
        "epub_2": "./datasets/examples/nested-myths.epub",
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


@pytest.mark.pipeline
@pytest.mark.order(1)
@pytest.mark.dependency(name="linear_01", scope="session")
# TODO: extract epub_1 from fixture 1, epub_2 from fixture 2, etc
def test_linear_01_convert_epub(example_data, epub_key):
    """Test EPUB -> TEI conversion."""
    tei_path = linear_01_convert_epub(example_data[epub_key])
    assert tei_path.endswith(".tei")
    # Verify file exists:
    assert os.path.exists(tei_path)


@pytest.mark.pipeline
@pytest.mark.order(2)
@pytest.mark.dependency(name="linear_02", scope="session")
def test_linear_02_parse_chapters(example_data):
    """Test TEI -> Story parsing."""
    tei_path = linear_01_convert_epub(example_data["epub_1"])
    story = linear_02_parse_chapters(
        tei_path,
        example_data["chapters"],
        example_data["book_id"],
        example_data["story_id"],
        example_data["start"],
        example_data["end"],
    )
    assert isinstance(story, Story)
    assert hasattr(story, "reader")


@pytest.mark.pipeline
@pytest.mark.order(3)
@pytest.mark.dependency(name="linear_03", scope="session")
def test_linear_03_chunk_story(example_data):
    """Test Story -> chunks splitting."""
    tei_path = linear_01_convert_epub(example_data["epub_1"])
    story = linear_02_parse_chapters(
        tei_path,
        example_data["chapters"],
        example_data["book_id"],
        example_data["story_id"],
        example_data["start"],
        example_data["end"],
    )
    chunks = linear_03_chunk_story(story)
    assert isinstance(chunks, list)
    assert len(chunks) > 0
    for c in chunks:
        assert hasattr(c, "text")
        assert len(c.text) > 0



