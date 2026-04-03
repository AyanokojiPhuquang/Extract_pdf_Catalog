"""Unit tests for cost.py module."""

from cost import BatchCost, SessionCost, calculate_batch_cost, accumulate_cost


class TestCalculateBatchCost:
    def test_valid_usage(self):
        usage = {"prompt_tokens": 1000, "completion_tokens": 500}
        result = calculate_batch_cost(usage, prompt_price=0.000003, completion_price=0.000015)
        assert result is not None
        assert result.prompt_tokens == 1000
        assert result.completion_tokens == 500
        assert result.cost_usd == 1000 * 0.000003 + 500 * 0.000015

    def test_missing_usage_none(self):
        assert calculate_batch_cost(None, 0.000003, 0.000015) is None

    def test_empty_dict(self):
        assert calculate_batch_cost({}, 0.000003, 0.000015) is None

    def test_missing_prompt_tokens_key(self):
        usage = {"completion_tokens": 500}
        assert calculate_batch_cost(usage, 0.000003, 0.000015) is None

    def test_missing_completion_tokens_key(self):
        usage = {"prompt_tokens": 1000}
        assert calculate_batch_cost(usage, 0.000003, 0.000015) is None

    def test_zero_tokens(self):
        usage = {"prompt_tokens": 0, "completion_tokens": 0}
        result = calculate_batch_cost(usage, 0.000003, 0.000015)
        assert result is not None
        assert result.cost_usd == 0.0

    def test_non_dict_usage(self):
        assert calculate_batch_cost("not a dict", 0.000003, 0.000015) is None
        assert calculate_batch_cost(42, 0.000003, 0.000015) is None
        assert calculate_batch_cost([], 0.000003, 0.000015) is None


class TestAccumulateCost:
    def test_accumulate_single_batch(self):
        session = SessionCost(total_prompt_tokens=0, total_completion_tokens=0, total_cost_usd=0.0)
        batch = BatchCost(prompt_tokens=100, completion_tokens=50, cost_usd=0.005)
        result = accumulate_cost(session, batch)
        assert result.total_prompt_tokens == 100
        assert result.total_completion_tokens == 50
        assert result.total_cost_usd == 0.005

    def test_accumulate_multiple_batches(self):
        session = SessionCost(total_prompt_tokens=100, total_completion_tokens=50, total_cost_usd=0.005)
        batch = BatchCost(prompt_tokens=200, completion_tokens=100, cost_usd=0.01)
        result = accumulate_cost(session, batch)
        assert result.total_prompt_tokens == 300
        assert result.total_completion_tokens == 150
        assert result.total_cost_usd == 0.015
