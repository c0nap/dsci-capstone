import pytest
from src.core.stages import *

@pytest.mark.pipeline
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="task_12_rebel_minimal", scope="session")
def test_task_12_rebel_minimal():
    """Runs REBEL on a basic example."""
    sample_text = "Alice met Bob in the forest. Bob then went to the village."
    extracted = task_12_relation_extraction_rebel(sample_text, parse_tuples=False)
    
    assert isinstance(extracted, list)
    assert len(extracted) > 0
    # REBEL returns raw strings when parse_tuples=False
    assert all(isinstance(triple, str) for triple in extracted)


@pytest.mark.pipeline
@pytest.mark.smoke
@pytest.mark.order(12)
@pytest.mark.dependency(name="task_12_rebel_tuples", scope="session")
def test_task_12_rebel_with_tuples():
    """Runs REBEL with tuple parsing enabled."""
    sample_text = "The Phoenix told the children about the magic carpet."
    extracted = task_12_relation_extraction_rebel(sample_text, parse_tuples=True)
    
    assert isinstance(extracted, list)
    assert len(extracted) > 0
    # With parse_tuples=True, returns (subject, relation, object) tuples
    assert all(isinstance(triple, tuple) and len(triple) == 3 for triple in extracted)


@pytest.mark.pipeline
@pytest.mark.smoke
@pytest.mark.order(14)
@pytest.mark.dependency(name="task_14_llm_minimal", scope="session")
def test_task_14_llm_minimal():
    """Test LLM-based triple sanitization with minimal input."""
    triples_string = "Alice  met  Bob\nBob  went_to  village"
    text = "Alice met Bob in the forest. Bob then went to the village."
    
    prompt, llm_output = task_14_relation_extraction_llm(triples_string, text)
    
    assert isinstance(prompt, str)
    assert triples_string in prompt
    assert isinstance(llm_output, str)
    assert len(llm_output) > 0






@pytest.mark.pipeline
@pytest.mark.smoke
@pytest.mark.order(4)
@pytest.mark.dependency(name="pipeline_B_minimal", scope="session")
def test_pipeline_B_minimal():
    """Test running the aggregate pipeline_A on a single book."""
    pass

