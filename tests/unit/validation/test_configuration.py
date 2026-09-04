from __future__ import annotations

import pytest

from millrace.validation.configuration import IdentifierCase, quote_identifier, quote_relation


def test_preserve_case_is_unchanged_from_before_snowflake_support() -> None:
    assert quote_identifier("customer_id") == '"customer_id"'
    assert quote_relation("candidate.validation_customers") == '"candidate"."validation_customers"'


def test_upper_case_folds_identifiers_for_snowflake() -> None:
    assert quote_identifier("customer_id", case=IdentifierCase.UPPER) == '"CUSTOMER_ID"'
    assert (
        quote_relation("candidate.validation_customers", case=IdentifierCase.UPPER)
        == '"CANDIDATE"."VALIDATION_CUSTOMERS"'
    )


def test_unsafe_identifiers_are_still_rejected_regardless_of_case() -> None:
    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        quote_identifier("drop table; --", case=IdentifierCase.UPPER)
