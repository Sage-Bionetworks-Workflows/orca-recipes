import pandas as pd

from dags.src.dois.doi_to_datacatalog import _build_prompt, _is_empty, transform_synapse_dois


class TestIsEmpty:
    def test_none(self) -> None:
        """None is empty."""
        assert _is_empty(None) is True

    def test_nan(self) -> None:
        """NaN float is empty."""
        assert _is_empty(float("nan")) is True

    def test_empty_string(self) -> None:
        """Empty string is empty."""
        assert _is_empty("") is True

    def test_whitespace_string(self) -> None:
        """Whitespace-only string is empty."""
        assert _is_empty("   ") is True

    def test_empty_list(self) -> None:
        """Empty list is empty."""
        assert _is_empty([]) is True

    def test_non_empty_string(self) -> None:
        """Non-empty string is not empty."""
        assert _is_empty("hello") is False

    def test_non_empty_list(self) -> None:
        """Non-empty list is not empty."""
        assert _is_empty(["a"]) is False

    def test_zero(self) -> None:
        """Zero integer is not empty."""
        assert _is_empty(0) is False

    def test_false(self) -> None:
        """Boolean False is not empty."""
        assert _is_empty(False) is False

    def test_integer(self) -> None:
        """Non-zero integer is not empty."""
        assert _is_empty(42) is False


class TestTransformSynapseDois:
    def test_parses_json_string(self) -> None:
        """JSON string in ANNOTATIONS column is parsed into a dict."""
        df = pd.DataFrame({"ANNOTATIONS": ['{"key": "value"}']})
        result = transform_synapse_dois(df)
        assert result["ANNOTATIONS"].iloc[0] == {"key": "value"}

    def test_leaves_dict_unchanged(self) -> None:
        """Dict in ANNOTATIONS column is left as-is."""
        df = pd.DataFrame({"ANNOTATIONS": [{"key": "value"}]})
        result = transform_synapse_dois(df)
        assert result["ANNOTATIONS"].iloc[0] == {"key": "value"}


class TestBuildPrompt:
    def test_includes_entity_context(self) -> None:
        """Prompt includes entity ID, name, node type, and requested fields."""
        prompt = _build_prompt(
            entity_id="syn123",
            name="My Dataset",
            node_type="dataset",
            wiki_markdown="",
            want_description=True,
            want_keywords=False,
        )
        assert "syn123" in prompt
        assert "My Dataset" in prompt
        assert "dataset" in prompt
        assert "description" in prompt

    def test_requests_keywords_field(self) -> None:
        """Prompt requests keywords with guidance and omits description when only keywords wanted."""
        prompt = _build_prompt(
            entity_id="syn123",
            name="My Dataset",
            node_type="dataset",
            wiki_markdown="",
            want_description=False,
            want_keywords=True,
        )
        assert "keywords" in prompt
        assert "Disease/condition" in prompt
        assert "description" not in prompt

    def test_excludes_keyword_guidance_when_not_wanted(self) -> None:
        """Keyword category guidance is omitted when want_keywords is False."""
        prompt = _build_prompt(
            entity_id="syn123",
            name="My Dataset",
            node_type="dataset",
            wiki_markdown="",
            want_description=True,
            want_keywords=False,
        )
        assert "Disease/condition" not in prompt

    def test_includes_wiki_when_provided(self) -> None:
        """Wiki markdown is included in the prompt when non-empty."""
        prompt = _build_prompt(
            entity_id="syn123",
            name="My Dataset",
            node_type="dataset",
            wiki_markdown="Wiki content here",
            want_description=True,
            want_keywords=False,
        )
        assert "Wiki content here" in prompt

    def test_excludes_wiki_when_empty(self) -> None:
        """Wiki section is omitted when wiki_markdown is empty."""
        prompt = _build_prompt(
            entity_id="syn123",
            name="My Dataset",
            node_type="dataset",
            wiki_markdown="",
            want_description=True,
            want_keywords=False,
        )
        assert "Wiki:" not in prompt

    def test_truncates_wiki_to_2000_chars(self) -> None:
        """Wiki markdown is truncated to 2000 characters in the prompt."""
        prompt = _build_prompt(
            entity_id="syn123",
            name="My Dataset",
            node_type="dataset",
            wiki_markdown="x" * 3000,
            want_description=True,
            want_keywords=False,
        )
        assert "x" * 2000 in prompt
        assert "x" * 2001 not in prompt
