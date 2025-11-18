import pytest
import src.core.stages

@pytest.mark.pipeline
@pytest.mark.smoke
@pytest.mark.order(1)
@pytest.mark.dependency(name="task_12_rebel_minimal", scope="session")
def test_task_12_rebel_minimal(book_data):
    """Runs REBEL on a basic example."""
    sample_text = "Alice met Bob in the forest. Bob then went to the village."
    extracted = task_12_relation_extraction_rebel(sample_text, parse_tuples=False)

    assert True # TODO
