from __future__ import annotations

from pytest_balance.algorithms.partitioner import Scope, group_by_scope


class TestGroupByScope:
    def test_group_by_module(self):
        tests = [
            "tests/test_api.py::test_get",
            "tests/test_api.py::test_post",
            "tests/test_auth.py::test_login",
        ]
        groups = group_by_scope(tests, Scope.MODULE)
        assert len(groups) == 2
        api_group = next(g for g in groups if g.scope_id == "tests/test_api.py")
        assert api_group.test_ids == [
            "tests/test_api.py::test_get",
            "tests/test_api.py::test_post",
        ]

    def test_group_by_class(self):
        tests = [
            "tests/test_foo.py::test_standalone",
            "tests/test_foo.py::TestBar::test_method",
            "tests/test_foo.py::TestBar::test_other",
        ]
        groups = group_by_scope(tests, Scope.CLASS)
        assert len(groups) == 2
        bar_group = next(g for g in groups if "TestBar" in g.scope_id)
        assert len(bar_group.test_ids) == 2

    def test_group_by_test(self):
        tests = ["tests/test_a.py::test_1", "tests/test_a.py::test_2"]
        groups = group_by_scope(tests, Scope.TEST)
        assert len(groups) == 2
        assert all(len(g.test_ids) == 1 for g in groups)

    def test_group_by_group_marker(self):
        tests = [
            "tests/test_a.py::test_1@group_a",
            "tests/test_b.py::test_2@group_a",
            "tests/test_c.py::test_3@group_b",
            "tests/test_d.py::test_4",
        ]
        groups = group_by_scope(tests, Scope.GROUP)
        group_a = next(g for g in groups if g.scope_id == "group_a")
        assert len(group_a.test_ids) == 2
        ungrouped = [g for g in groups if g.scope_id == "tests/test_d.py::test_4"]
        assert len(ungrouped) == 1

    def test_preserves_order_within_groups(self):
        tests = [
            "tests/test_a.py::test_3",
            "tests/test_a.py::test_1",
            "tests/test_a.py::test_2",
        ]
        groups = group_by_scope(tests, Scope.MODULE)
        assert groups[0].test_ids == tests

    def test_empty_input(self):
        assert group_by_scope([], Scope.MODULE) == []
