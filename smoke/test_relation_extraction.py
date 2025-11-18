import pytest


@pytest.mark.pipeline
@pytest.mark.smoke
@pytest.mark.order(1)
@pytest.mark.dependency(name="task_01", scope="session")
@pytest.mark.parametrize("book_data", ["book_1_data", "book_2_data"], indirect=True)
def test_task_01_convert_epub(book_data):
    """Test EPUB -> TEI conversion for multiple books."""
    tei_path = task_01_convert_epub(book_data["epub"])
    assert tei_path.endswith(".tei")
    assert os.path.exists(tei_path)

def test_relation_extraction():
    """Runs REBEL on a basic example; used for debugging."""
    from components.text_processing import RelationExtractor

    sample_text = "Alice met Bob in the forest. Bob then went to the village."
    extractor = RelationExtractor(model_name="Babelscape/rebel-large")
    print(extractor.extract(sample_text))