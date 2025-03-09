#!/usr/bin/env python3
"""
Test Migration Helper Script using LibCST

This script helps migrate legacy unittest.TestCase tests to pytest style
using LibCST for robust code transformations that understand Python's syntax.

Usage:
    python scripts/migrate_test_libcst.py --file tests/unit/test_skype_parser.py --output tests/unit/test_skype_parser_pytest.py

Features:
- Converts unittest.TestCase classes to pytest-style functions
- Converts setUp/tearDown methods to pytest fixtures
- Converts unittest assertions to pytest assertions
- Preserves comments and formatting
- Handles imports properly
"""

import argparse
import logging
import os
import sys
import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any, Union, Callable, cast

import libcst as cst
import libcst.matchers as m
from libcst.metadata import MetadataWrapper, PositionProvider

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)
logger = logging.getLogger("migrate_test_libcst")

# Mapping of unittest assertion methods to pytest assertion operators/wrappers
ASSERTION_METHODS = {
    "assertEqual": ["=="],
    "assertEquals": ["=="],  # Alias for assertEqual
    "assertNotEqual": ["!="],
    "assertIs": ["is"],
    "assertIsNot": ["is not"],
    "assertIn": ["in"],
    "assertNotIn": ["not in"],
    "assertTrue": [""],
    "assertFalse": ["not "],
    "assertIsNone": ["is None"],
    "assertIsNotNone": ["is not None"],
    "assertGreater": [">"],
    "assertGreaterEqual": [">="],
    "assertLess": ["<"],
    "assertLessEqual": ["<="],
    "assertAlmostEqual": ["", "", "pytest.approx"],
    "assertNotAlmostEqual": ["!=", "", "pytest.approx"],
    "assertDictEqual": ["=="],
    "assertDictContainsSubset": ["=="],  # This is simplified, might need special handling
    "assertListEqual": ["=="],
    "assertTupleEqual": ["=="],
    "assertSetEqual": ["=="],
    "assertMultiLineEqual": ["=="],
    "assertSequenceEqual": ["=="],
    "assertRaises": ["pytest.raises"],
    "assertIsInstance": ["isinstance"],  # Add support for assertIsInstance
}

# Decorator mappings - maps unittest decorators to pytest decorators
DECORATOR_MAPPINGS = {
    "unittest.skip": "pytest.mark.skip",
    "skip": "pytest.mark.skip",
    "unittest.skipIf": "pytest.mark.skipif",
    "skipIf": "pytest.mark.skipif",
    "unittest.skipUnless": "pytest.mark.skipif",  # Need to invert the condition
    "skipUnless": "pytest.mark.skipif",  # Need to invert the condition
    "unittest.expectedFailure": "pytest.mark.xfail",
    "expectedFailure": "pytest.mark.xfail",
    # New mock decorators - these will be handled specially
    "unittest.mock.patch": "__special_mock__",
    "mock.patch": "__special_mock__",
    "patch": "special:mock",
    "unittest.mock.patch.object": "__special_mock__",
    "mock.patch.object": "__special_mock__",
    "patch.object": "special:mock_object",
    "unittest.mock.patch.dict": "__special_mock__",
    "mock.patch.dict": "__special_mock__",
    "patch.dict": "special:mock_dict",
}

# Context manager mappings - maps unittest context managers to pytest equivalents
CONTEXT_MANAGER_MAPPINGS = {
    "assertRaises": "pytest.raises",
    "assertWarns": "pytest.warns",
    "assertLogs": "pytest.LogCaptureFixture",  # This requires more complex handling
    "assertNoLogs": None,  # No direct equivalent
}


class AssertionTransformer:
    """Base class for transforming unittest assertions to pytest assertions."""

    @classmethod
    def can_handle(cls, method_name: str) -> bool:
        """Check if this transformer can handle a given assertion method."""
        return method_name in cls.handled_methods()

    @classmethod
    def handled_methods(cls) -> List[str]:
        """Return a list of method names this transformer can handle."""
        return []

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        """Transform a unittest assertion to a pytest assertion."""
        raise NotImplementedError("Subclasses must implement this method")


class ComparisonAssertionTransformer(AssertionTransformer):
    """Transform comparison assertions like assertEqual, assertIs, etc."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return [
            "assertEqual", "assertEquals", "assertNotEqual", "assertIs", "assertIsNot",
            "assertIn", "assertNotIn", "assertGreater", "assertGreaterEqual",
            "assertLess", "assertLessEqual"
        ]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 2:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs 2 args, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        # Extract the comparison components
        left = args[0].value
        right = args[1].value

        operator = ASSERTION_METHODS[method_name][0]

        # Handle compound operators like "is not"
        if " " in operator:
            if operator == "is not":
                comp_op = cst.IsNot()
            elif operator == "not in":
                comp_op = cst.NotIn()
            else:
                todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} Convert {method_name}"
                return cst.Assert(
                    test=cst.SimpleString(f'"{todo_msg}"'),
                    msg=error_msg
                )
        else:
            # Handle simple operators
            op_map = {
                "==": cst.Equal(),
                "!=": cst.NotEqual(),
                ">": cst.GreaterThan(),
                ">=": cst.GreaterThanEqual(),
                "<": cst.LessThan(),
                "<=": cst.LessThanEqual(),
                "in": cst.In(),
                "is": cst.Is(),
            }
            comp_op = op_map.get(operator, cst.Equal())  # Default to == if not found

        test_expr = cst.Comparison(
            left=left,
            comparisons=[cst.ComparisonTarget(operator=comp_op, comparator=right)]
        )
        return cst.Assert(test=test_expr, msg=error_msg)


class SingleArgAssertionTransformer(AssertionTransformer):
    """Transform single-argument assertions like assertTrue, assertIsNone, etc."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return ["assertTrue", "assertFalse", "assertIsNone", "assertIsNotNone"]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 1:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs 1 arg, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        # Extract operation parts
        test_expr = args[0].value
        operator = ASSERTION_METHODS[method_name][0]

        if operator.startswith("not "):
            # For negated assertions
            test_expr = cst.UnaryOperation(
                operator=cst.Not(),
                expression=test_expr
            )
        elif operator.endswith("None"):
            # For is None / is not None
            test_expr = cst.Comparison(
                left=test_expr,
                comparisons=[
                    cst.ComparisonTarget(
                        operator=cst.Is() if operator == "is None" else cst.IsNot(),
                        comparator=cst.Name("None")
                    )
                ]
            )

        return cst.Assert(test=test_expr, msg=error_msg)


class ApproxAssertionTransformer(AssertionTransformer):
    """Transform approximate equality assertions like assertAlmostEqual."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return ["assertAlmostEqual", "assertNotAlmostEqual"]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 2:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs 2 args, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        # Get components
        left = args[0].value
        right = args[1].value

        # Handle the approx part with pytest.approx
        wrap_func = ASSERTION_METHODS[method_name][2] if len(ASSERTION_METHODS[method_name]) > 2 else None

        if wrap_func:
            approx_kwargs = []

            # Check for keyword arguments like delta, places
            for keyword_arg in keywords:
                if keyword_arg.keyword and keyword_arg.keyword.value == "delta":
                    # Add delta parameter to approx
                    approx_kwargs.append(
                        cst.Arg(
                            keyword=cst.Name("abs"),
                            value=keyword_arg.value,
                            equal=cst.AssignEqual(
                                whitespace_before=cst.SimpleWhitespace(""),
                                whitespace_after=cst.SimpleWhitespace("")
                            )
                        )
                    )
                elif keyword_arg.keyword and keyword_arg.keyword.value == "places":
                    # Convert places to abs with appropriate scaling
                    # For places=N, delta=10^-N
                    places_val = keyword_arg.value

                    # Add a comment to the error message about manual conversion
                    comment_msg = cst.SimpleString(f'" (Note: places={places_val.value if hasattr(places_val, "value") else "?"} parameter needs manual conversion to abs=10^-places)"')
                    if error_msg:
                        error_msg = cst.BinaryOperation(
                            left=error_msg,
                            operator=cst.Add(),
                            right=comment_msg
                        )
                    else:
                        error_msg = comment_msg

                    # Try to create a power operation if we can extract the integer value
                    # This is a best effort conversion
                    try:
                        if isinstance(places_val, cst.Integer):
                            places_int = int(places_val.value)
                            abs_val = 10 ** (-places_int)
                            approx_kwargs.append(
                                cst.Arg(
                                    keyword=cst.Name("abs"),
                                    value=cst.Float(value=str(abs_val)),
                                    equal=cst.AssignEqual(
                                        whitespace_before=cst.SimpleWhitespace(""),
                                        whitespace_after=cst.SimpleWhitespace("")
                                    )
                                )
                            )
                    except (ValueError, TypeError):
                        # If we can't convert, leave the comment for manual intervention
                        pass

            # Create the pytest.approx call
            right = cst.Call(
                func=cst.Name(wrap_func),
                args=[cst.Arg(value=right)],
                keywords=approx_kwargs
            )

        # Create the comparison expression
        operator = ASSERTION_METHODS[method_name][0]
        op_map = {"": cst.Equal(), "!=": cst.NotEqual()}
        comp_op = op_map.get(operator, cst.Equal())

        test_expr = cst.Comparison(
            left=left,
            comparisons=[cst.ComparisonTarget(operator=comp_op, comparator=right)]
        )

        return cst.Assert(test=test_expr, msg=error_msg)


class DictAssertionTransformer(AssertionTransformer):
    """Transform dictionary-specific assertions."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return ["assertDictEqual", "assertDictContainsSubset"]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 2:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs 2 args, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        left = args[0].value
        right = args[1].value

        # For assertDictContainsSubset, we might need more complex logic
        # but a simple equality check will work for most cases
        test_expr = cst.Comparison(
            left=left,
            comparisons=[cst.ComparisonTarget(operator=cst.Equal(), comparator=right)]
        )

        if method_name == "assertDictContainsSubset":
            # Add a comment to check the conversion
            if error_msg:
                error_msg = cst.BinaryOperation(
                    left=error_msg,
                    operator=cst.Add(),
                    right=cst.SimpleString('" (Note: Check that dict subset logic is preserved)"')
                )
            else:
                error_msg = cst.SimpleString('"Note: Check that dict subset logic is preserved"')

        return cst.Assert(test=test_expr, msg=error_msg)


class SequenceAssertionTransformer(AssertionTransformer):
    """Transform sequence-specific assertions."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return ["assertListEqual", "assertSequenceEqual", "assertTupleEqual", "assertSetEqual", "assertMultiLineEqual"]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 2:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs 2 args, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        left = args[0].value
        right = args[1].value

        test_expr = cst.Comparison(
            left=left,
            comparisons=[cst.ComparisonTarget(operator=cst.Equal(), comparator=right)]
        )

        return cst.Assert(test=test_expr, msg=error_msg)


class RaisesAssertionTransformer(AssertionTransformer):
    """Transform assertRaises assertions."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return ["assertRaises"]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 2:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs at least 2 args, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        # Convert to simple form: assert pytest.raises(Exception, func, *args)
        exception_cls = args[0].value
        callable_obj = args[1].value
        call_args = args[2:]

        test_expr = cst.Call(
            func=cst.Attribute(
                value=cst.Name("pytest"),
                attr=cst.Name("raises")
            ),
            args=[
                cst.Arg(value=exception_cls),
                cst.Arg(value=callable_obj),
            ] + call_args
        )

        return cst.Assert(test=test_expr, msg=error_msg)


class InstanceAssertionTransformer(AssertionTransformer):
    """Transform assertIsInstance assertions."""

    @classmethod
    def handled_methods(cls) -> List[str]:
        return ["assertIsInstance"]

    @classmethod
    def transform(
        cls, method_name: str, args: List[cst.Arg],
        keywords: List[cst.Arg], error_msg: Optional[cst.BaseExpression],
        line_info: Optional[int] = None
    ) -> cst.Assert:
        if len(args) < 2:
            todo_msg = f"TODO:{' (Line ' + str(line_info) + ')' if line_info else ''} {method_name} needs 2 args, got {len(args)}"
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=error_msg
            )

        # Extract arguments - first is the object, second is the expected type
        obj = args[0].value
        expected_type = args[1].value

        # Create the isinstance call
        test_expr = cst.Call(
            func=cst.Name("isinstance"),
            args=[
                cst.Arg(value=obj),
                cst.Arg(value=expected_type)
            ]
        )

        return cst.Assert(test=test_expr, msg=error_msg)


# Register all transformers
ASSERTION_TRANSFORMERS = [
    ComparisonAssertionTransformer,
    SingleArgAssertionTransformer,
    ApproxAssertionTransformer,
    DictAssertionTransformer,
    SequenceAssertionTransformer,
    RaisesAssertionTransformer,
    InstanceAssertionTransformer,  # Add the new transformer
]


class TestCaseVisitor(cst.CSTVisitor):
    """
    Visitor to collect information about TestCase classes, setUp/tearDown methods,
    and test methods from a python file.
    """

    METADATA_DEPENDENCIES = (PositionProvider,)

    def __init__(self) -> None:
        super().__init__()
        self.test_classes: List[Dict[str, Any]] = []
        self.current_class: Optional[Dict[str, Any]] = None
        self.has_test_case_import = False
        self.imports: List[cst.Import] = []
        self.import_froms: List[cst.ImportFrom] = []
        # Track known TestCase base classes (including custom ones)
        self.testcase_bases = {"TestCase", "unittest.TestCase"}
        # Track test method patterns
        self.test_method_prefixes = {"test_"}
        # Track test method decorators
        self.test_decorators = {"unittest.skip", "unittest.skipIf", "unittest.skipUnless", "pytest.mark.skip"}
        # Track mock patches that will need to be converted to fixtures
        self.mock_patches = {}  # Mapping of test method names to their patch decorators
        # Track if pytest-mock is needed
        self.needs_pytest_mock = False

    def visit_Import(self, node: cst.Import) -> None:
        self.imports.append(node)

    def visit_ImportFrom(self, node: cst.ImportFrom) -> None:
        self.import_froms.append(node)
        if m.matches(node, m.ImportFrom(module=m.Name("unittest"))) and any(
            m.matches(imp, m.ImportAlias(name=m.Name("TestCase")))
            for imp in node.names
        ):
            self.has_test_case_import = True

    def _is_testcase_class(self, node: cst.ClassDef) -> bool:
        """
        Check if a class is a TestCase class or subclass.
        Handles direct inheritance, unittest.TestCase, and known custom TestCase subclasses.
        """
        if not node.bases:
            return False

        for base in node.bases:
            # Direct TestCase inheritance
            if isinstance(base.value, cst.Name) and base.value.value == "TestCase":
                return True

            # unittest.TestCase inheritance
            if (isinstance(base.value, cst.Attribute) and
                isinstance(base.value.value, cst.Name) and
                base.value.value.value == "unittest" and
                base.value.attr.value == "TestCase"):
                return True

            # Check for known custom TestCase bases
            base_name = ""
            if isinstance(base.value, cst.Name):
                base_name = base.value.value
            elif isinstance(base.value, cst.Attribute) and isinstance(base.value.attr, cst.Name):
                if isinstance(base.value.value, cst.Name):
                    base_name = f"{base.value.value.value}.{base.value.attr.value}"

            if base_name in self.testcase_bases:
                return True

        return False

    def _is_test_method(self, node: cst.FunctionDef) -> bool:
        """
        Check if a function is a test method.
        Handles methods starting with test_ or with test decorators.
        """
        # Check name prefix
        for prefix in self.test_method_prefixes:
            if node.name.value.startswith(prefix):
                return True

        # Check decorators
        for decorator in node.decorators:
            if isinstance(decorator.decorator, cst.Name):
                # Simple decorator like @test
                if decorator.decorator.value in self.test_decorators:
                    return True
            elif isinstance(decorator.decorator, cst.Attribute):
                # Attribute decorator like @unittest.skip
                if isinstance(decorator.decorator.value, cst.Name):
                    decorator_name = f"{decorator.decorator.value.value}.{decorator.decorator.attr.value}"
                    if decorator_name in self.test_decorators:
                        return True

        return False

    def visit_ClassDef(self, node: cst.ClassDef) -> None:
        # Check if this is a TestCase class
        if self._is_testcase_class(node):
            class_name = node.name.value
            self.current_class = {
                "name": class_name,
                "node": node,
                "setup": None,
                "teardown": None,
                "setup_class": None,
                "teardown_class": None,
                "tests": [],
                "position": self.get_metadata(PositionProvider, node).start,
                # Store all decorators to preserve them
                "decorators": node.decorators,
            }
            # Add this class to our known TestCase bases so we can detect subclasses
            self.testcase_bases.add(class_name)
            self.test_classes.append(self.current_class)

    def leave_ClassDef(self, original_node: cst.ClassDef) -> None:
        self.current_class = None

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        if self.current_class is None:
            return

        function_name = node.name.value

        # Check if this is setUp or tearDown
        if function_name == "setUp":
            self.current_class["setup"] = node
        elif function_name == "tearDown":
            self.current_class["teardown"] = node
        elif function_name == "setUpClass":
            self.current_class["setup_class"] = node
        elif function_name == "tearDownClass":
            self.current_class["teardown_class"] = node
        # Check if this is a test method
        elif self._is_test_method(node):
            self.current_class["tests"].append(node)

    def add_todo_comment(self, todo_text: str, line_num: Optional[int] = None) -> str:
        """
        Create a standardized TODO comment with location information if available.

        Args:
            todo_text: The specific TODO message
            line_num: The line number where the TODO applies, if known

        Returns:
            A formatted TODO string
        """
        location_info = f" (Line {line_num})" if line_num is not None else ""
        return f"TODO:{location_info} {todo_text}"


class UnittestToTestReplacer(cst.CSTTransformer):
    """
    Transformer to convert unittest.TestCase classes to pytest-style tests.
    """

    def __init__(
        self,
        test_classes: List[Dict[str, Any]],
        imports: List[cst.Import],
        import_froms: List[cst.ImportFrom]
    ) -> None:
        self.test_classes = test_classes
        self.imports = imports
        self.import_froms = import_froms
        self.fixtures = []
        self.current_class = None
        self.has_found_pytest = False
        self.pytest_imports = []
        self.function_fixtures = {}
        self.class_fixtures = {}
        self.is_test_class = False
        self.needs_pytest_mock = False  # Add this to track if pytest-mock is needed
        self.class_names = {cls["name"]: i for i, cls in enumerate(test_classes)}
        self.current_class_idx = -1
        self.fixtures_to_add = []
        self.test_functions_to_add = []
        self.class_fixtures_to_add = []  # For class-level fixtures (scope="class")

    def add_todo_comment(self, todo_text: str, line_num: Optional[int] = None) -> str:
        """
        Create a standardized TODO comment with location information if available.

        Args:
            todo_text: The specific TODO message
            line_num: The line number where the TODO applies, if known

        Returns:
            A formatted TODO string
        """
        location_info = f" (Line {line_num})" if line_num is not None else ""
        return f"TODO:{location_info} {todo_text}"

    def _create_fixture_from_setup_teardown(
        self, class_name: str, setup_node: Optional[cst.FunctionDef], teardown_node: Optional[cst.FunctionDef],
        is_class_fixture: bool = False
    ) -> cst.FunctionDef:
        """Create a pytest fixture from setUp/tearDown or setUpClass/tearDownClass methods."""
        if is_class_fixture:
            fixture_name = f"{class_name.lower().replace('test', '')}_class_setup"
            scope = "class"
        else:
            fixture_name = f"{class_name.lower().replace('test', '')}_setup"
            scope = "function"  # Default pytest fixture scope

        # Extract setup body, removing self references
        setup_body = []
        if setup_node:
            for stmt in setup_node.body.body:
                setup_body.append(self._remove_self_reference(stmt))

        # Extract teardown body, removing self references
        teardown_body = []
        if teardown_node:
            for stmt in teardown_node.body.body:
                teardown_body.append(self._remove_self_reference(stmt))

        # Create the fixture function with yield
        body = setup_body + [
            cst.SimpleStatementLine(body=[cst.Expr(value=cst.Name("yield"))])
        ] + teardown_body

        # Add the @pytest.fixture decorator with appropriate scope
        fixture_decorator = cst.Decorator(
            decorator=cst.Call(
                func=cst.Attribute(
                    value=cst.Name("pytest"),
                    attr=cst.Name("fixture")
                ),
                args=[
                    cst.Arg(
                        keyword=cst.Name("scope"),
                        value=cst.SimpleString(f'"{scope}"'),
                        equal=cst.AssignEqual(
                            whitespace_before=cst.SimpleWhitespace(""),
                            whitespace_after=cst.SimpleWhitespace("")
                        )
                    )
                ]
            ),
            trailing_whitespace=cst.SimpleWhitespace(" ")  # Simple space instead of newline
        )

        # Create the fixture function with proper leading lines to ensure spacing
        return cst.FunctionDef(
            name=cst.Name(fixture_name),
            params=cst.Parameters(params=[]),
            body=cst.IndentedBlock(body=body),
            decorators=[fixture_decorator],
            leading_lines=[cst.EmptyLine(), cst.EmptyLine()],  # Add empty lines for spacing
            returns=None,
            asynchronous=None  # Use None for synchronous functions
        )

    def _convert_unittest_decorator(self, decorator: cst.Decorator) -> Optional[cst.Decorator]:
        """
        Convert unittest decorators to pytest decorators.

        Args:
            decorator: The decorator to convert

        Returns:
            Converted decorator or None if it should be removed
        """
        # Check if this is a patch decorator that needs special handling
        if (isinstance(decorator.decorator, cst.Call) and
            isinstance(decorator.decorator.func, cst.Name) and
            decorator.decorator.func.value in ["patch", "patch.object", "patch.dict"]):

            # Add flag to indicate pytest-mock is needed
            self.needs_pytest_mock = True

            # Mark for manual conversion - we'll keep it as is but add a TODO comment
            return cst.Decorator(
                decorator=cst.Call(
                    func=decorator.decorator.func,
                    args=decorator.decorator.args,
                    keywords=[
                        *decorator.decorator.keywords,
                        cst.Arg(
                            keyword=cst.Name("__TODO__"),
                            value=cst.SimpleString('"Convert to pytest fixture using mocker"'),
                            equal=cst.AssignEqual()
                        )
                    ]
                ),
                whitespace_after_at=cst.SimpleWhitespace(" "),
                whitespace_before_decorator=cst.SimpleWhitespace("")
            )

        # For attribute call like unittest.skip
        elif isinstance(decorator.decorator, cst.Call) and isinstance(decorator.decorator.func, cst.Attribute):
            # Get the full path of the decorator
            decorator_path = self._get_decorator_path(decorator.decorator.func)

            # Check if this is a mock patch
            if any(decorator_path.startswith(mock_path) for mock_path in ["unittest.mock.patch", "mock.patch"]):
                # Add flag to indicate pytest-mock is needed
                self.needs_pytest_mock = True

                # Keep as is but add a TODO comment
                return cst.Decorator(
                    decorator=cst.Call(
                        func=decorator.decorator.func,
                        args=decorator.decorator.args,
                        keywords=[
                            *decorator.decorator.keywords,
                            cst.Arg(
                                keyword=cst.Name("__TODO__"),
                                value=cst.SimpleString('"Convert to pytest fixture using mocker"'),
                                equal=cst.AssignEqual()
                            )
                        ]
                    ),
                    whitespace_after_at=cst.SimpleWhitespace(" "),
                    whitespace_before_decorator=cst.SimpleWhitespace("")
                )

            # Handle other unittest decorators
            if decorator_path in DECORATOR_MAPPINGS:
                target = DECORATOR_MAPPINGS[decorator_path]

                # Handle special cases for mock decorators
                if target.startswith("__special_mock__"):
                    self.needs_pytest_mock = True
                    # Keep decorator but add TODO comment
                    return cst.Decorator(
                        decorator=cst.Call(
                            func=decorator.decorator.func,
                            args=decorator.decorator.args,
                            keywords=[
                                *decorator.decorator.keywords,
                                cst.Arg(
                                    keyword=cst.Name("__TODO__"),
                                    value=cst.SimpleString('"Convert to pytest fixture using mocker"'),
                                    equal=cst.AssignEqual()
                                )
                            ]
                        ),
                        whitespace_after_at=cst.SimpleWhitespace(" "),
                        whitespace_before_decorator=cst.SimpleWhitespace("")
                    )

                # Handle skipUnless -> skipif conversion (inverted condition)
                elif decorator_path == "unittest.skipUnless":
                    # Extract the condition argument and invert it
                    if len(decorator.decorator.args) > 0:
                        condition = decorator.decorator.args[0].value
                        inverted_condition = cst.UnaryOperation(
                            operator=cst.Not(), expression=condition
                        )

                        # Create a new skipif decorator
                        return cst.Decorator(
                            decorator=cst.Call(
                                func=cst.Name("pytest.mark.skipif"),
                                args=[cst.Arg(value=inverted_condition)] + list(decorator.decorator.args[1:]),
                                keywords=decorator.decorator.keywords
                            ),
                            whitespace_after_at=cst.SimpleWhitespace(" "),
                            whitespace_before_decorator=cst.SimpleWhitespace("")
                        )

                # Standard conversion
                parts = target.split(".")
                if len(parts) == 1:
                    # Simple name
                    func = cst.Name(target)
                else:
                    # Attribute access
                    func = cst.parse_expression(target)

                return cst.Decorator(
                    decorator=cst.Call(
                        func=func,
                        args=decorator.decorator.args,
                        keywords=decorator.decorator.keywords
                    ),
                    whitespace_after_at=cst.SimpleWhitespace(" "),
                    whitespace_before_decorator=cst.SimpleWhitespace("")
                )

        # For simple names like @skip
        elif isinstance(decorator.decorator, cst.Name):
            name = decorator.decorator.value

            # Check if this is a mock patch
            if name in ["patch", "patch.object", "patch.dict"]:
                self.needs_pytest_mock = True
                # Keep as is but add TODO comment
                return decorator

            if name in DECORATOR_MAPPINGS:
                target = DECORATOR_MAPPINGS[name]

                # Handle special mock cases
                if target.startswith("__special_mock__"):
                    self.needs_pytest_mock = True
                    return decorator

                # Standard conversion
                parts = target.split(".")
                if len(parts) == 1:
                    # Simple name replacement
                    return cst.Decorator(
                        decorator=cst.Name(target),
                        whitespace_after_at=cst.SimpleWhitespace(" "),
                        whitespace_before_decorator=cst.SimpleWhitespace("")
                    )
                else:
                    # Attribute access
                    return cst.Decorator(
                        decorator=cst.parse_expression(target),
                        whitespace_after_at=cst.SimpleWhitespace(" "),
                        whitespace_before_decorator=cst.SimpleWhitespace("")
                    )

        # Default - keep decorator as is
        return decorator

    def _get_decorator_path(self, node: Union[cst.Name, cst.Attribute]) -> str:
        """
        Extract the full path of a decorator (e.g., 'unittest.mock.patch').

        Args:
            node: The Name or Attribute node representing the decorator

        Returns:
            The full path of the decorator as a string
        """
        if isinstance(node, cst.Name):
            return node.value

        if isinstance(node, cst.Attribute):
            if isinstance(node.value, cst.Name):
                return f"{node.value.value}.{node.attr.value}"
            elif isinstance(node.value, cst.Attribute):
                return f"{self._get_decorator_path(node.value)}.{node.attr.value}"

        # Default case for unsupported nodes
        return ""

    def _remove_self_reference(self, node: cst.CSTNode) -> cst.CSTNode:
        """Remove 'self.' references from node."""
        class SelfRemover(cst.CSTTransformer):
            def leave_Attribute(self, original_node: cst.Attribute, updated_node: cst.Attribute) -> cst.CSTNode:
                if (
                    isinstance(original_node.value, cst.Name) and
                    original_node.value.value == "self"
                ):
                    return original_node.attr
                return updated_node

        return node.visit(SelfRemover())

    def _convert_assert_method(self, node: cst.Call) -> cst.Assert:
        """
        Convert unittest assertion methods to pytest assertions.

        Args:
            node: The Call node representing a unittest assertion method call

        Returns:
            A pytest-style assertion node
        """
        if not isinstance(node.func, cst.Attribute) or not isinstance(node.func.value, cst.Name):
            return cst.Assert(test=node)  # Default fallback

        if node.func.value.value != "self":
            return cst.Assert(test=node)  # Not a self.assertX call

        method_name = node.func.attr.value

        # Try to get the line number from metadata if available
        line_info = None
        if hasattr(node, "position") and node.position:
            line_info = node.position.start.line

        if method_name not in ASSERTION_METHODS:
            # Add a comment for unknown assertions
            todo_msg = self.add_todo_comment(f"Convert to pytest style - unknown assertion: {method_name}", line_info)
            logger.warning(f"Unknown assertion method: {method_name}")
            return cst.Assert(
                test=cst.SimpleString(f'"{todo_msg}"'),
                msg=None
            )

        # Extract arguments and error message
        args = list(node.args)
        error_msg = self._extract_error_message(args, method_name)

        # Get keywords if available, otherwise use empty list
        keywords = getattr(node, "keywords", [])

        # Find an appropriate transformer
        for transformer_cls in ASSERTION_TRANSFORMERS:
            if transformer_cls.can_handle(method_name):
                return transformer_cls.transform(
                    method_name=method_name,
                    args=args,
                    keywords=keywords,
                    error_msg=error_msg,
                    line_info=line_info
                )

        # Fallback for unhandled assertions or complex cases
        todo_msg = self.add_todo_comment(f"Convert complex assertion: {method_name}", line_info)
        logger.warning(f"Complex assertion needs attention: {method_name}")
        return cst.Assert(
            test=cst.SimpleString(f'"{todo_msg}"'),
            msg=error_msg
        )

    def _extract_error_message(self, args: List[cst.Arg], method_name: str) -> Optional[cst.BaseExpression]:
        """
        Extract the error message from assertion method arguments.

        Args:
            args: The arguments to the assertion
            method_name: The assertion method name

        Returns:
            Optional error message expression
        """
        if method_name not in ASSERTION_METHODS:
            return None

        # Get the number of expected arguments for this assertion
        expected_arg_count = {
            "assertEqual": 2,
            "assertEquals": 2,
            "assertNotEqual": 2,
            "assertIs": 2,
            "assertIsNot": 2,
            "assertIn": 2,
            "assertNotIn": 2,
            "assertTrue": 1,
            "assertFalse": 1,
            "assertIsNone": 1,
            "assertIsNotNone": 1,
            "assertGreater": 2,
            "assertGreaterEqual": 2,
            "assertLess": 2,
            "assertLessEqual": 2,
            "assertAlmostEqual": 2,
            "assertNotAlmostEqual": 2,
            "assertDictEqual": 2,
            "assertDictContainsSubset": 2,
            "assertListEqual": 2,
            "assertTupleEqual": 2,
            "assertSetEqual": 2,
            "assertMultiLineEqual": 2,
            "assertSequenceEqual": 2,
            "assertRaises": 2,  # Exception and callable are required, other args are optional
            "assertIsInstance": 2,  # Exception and obj are required, other args are optional
        }.get(method_name, 2)  # Default to 2 arguments if not found

        # Error message is usually the last argument, after the required args for the assertion
        if len(args) > expected_arg_count:
            last_arg = args[-1]

            # Check that it's not a keyword argument
            if last_arg.keyword is None:
                # Remove the error message from args
                args.pop()
                return last_arg.value

        return None

    def _convert_test_method(
        self, method_node: cst.FunctionDef, fixture_names: List[str] = None, decorators: List[cst.Decorator] = None
    ) -> cst.FunctionDef:
        """
        Convert a unittest test method to a pytest test function.

        Args:
            method_node: The original test method node
            fixture_names: List of fixture names to include as parameters
            decorators: List of pytest decorators to apply
        """
        method_name = method_node.name.value

        # Create new parameter list, adding fixtures if needed
        params = []
        if fixture_names:
            for fixture_name in fixture_names:
                params.append(
                    cst.Param(name=cst.Name(fixture_name), annotation=None, default=None)
                )
        new_params = cst.Parameters(params=params)

        # Process method body - remove self references and convert assertions
        class MethodBodyTransformer(cst.CSTTransformer):
            def __init__(self, outer_self):
                self.outer_self = outer_self

            def leave_Attribute(self, original_node: cst.Attribute, updated_node: cst.Attribute) -> cst.CSTNode:
                if (
                    isinstance(original_node.value, cst.Name) and
                    original_node.value.value == "self"
                ):
                    # If it's a self attribute but not an assertion method, just return the attribute
                    attr_name = original_node.attr.value
                    if not attr_name.startswith("assert"):
                        return original_node.attr
                return updated_node

            def leave_Call(self, original_node: cst.Call, updated_node: cst.Call) -> cst.CSTNode:
                # Convert assertion methods
                if (
                    isinstance(original_node.func, cst.Attribute) and
                    isinstance(original_node.func.value, cst.Name) and
                    original_node.func.value.value == "self" and
                    original_node.func.attr.value.startswith("assert")
                ):
                    return self.outer_self._convert_assert_method(updated_node)
                return updated_node

            def leave_Expr(self, original_node: cst.Expr, updated_node: cst.Expr) -> cst.CSTNode:
                """
                Handle assertions like self.assertEqual(a, b) by ensuring they're properly
                converted to assert a == b without the 'self.' prefix.
                """
                # Check if this is an assertion expression (self.assertX(...))
                if isinstance(original_node.value, cst.Call):
                    call = original_node.value
                    if (
                        isinstance(call.func, cst.Attribute) and
                        isinstance(call.func.value, cst.Name) and
                        call.func.value.value == "self" and
                        call.func.attr.value.startswith("assert")
                    ):
                        # This is a self.assertX(...) call - convert to a pytest assertion
                        assert_node = self.outer_self._convert_assert_method(call)
                        return cst.Expr(value=assert_node)
                return updated_node

            def leave_With(self, original_node: cst.With, updated_node: cst.With) -> cst.CSTNode:
                """
                Transform 'with self.assertRaises' to 'with pytest.raises'.

                Args:
                    original_node: The original With node
                    updated_node: The updated With node

                Returns:
                    The transformed With node
                """
                # Check for with self.assertRaises
                for i, item in enumerate(updated_node.items):
                    # Access context_expr through the item attribute
                    if (isinstance(item.item, cst.Call) and
                        isinstance(item.item.func, cst.Attribute) and
                        isinstance(item.item.func.value, cst.Name) and
                        item.item.func.value.value == "self" and
                        item.item.func.attr.value == "assertRaises"):

                        # Extract the exception class from the first argument
                        if item.item.args:
                            exception_cls = item.item.args[0].value

                            # Create a new pytest.raises call
                            new_item = cst.WithItem(
                                item=cst.Call(
                                    func=cst.Attribute(
                                        value=cst.Name("pytest"),
                                        attr=cst.Name("raises")
                                    ),
                                    args=[cst.Arg(value=exception_cls)],
                                    keywords=[]
                                ),
                                asname=item.asname
                            )

                            # Update the with items
                            items = list(updated_node.items)
                            items[i] = new_item

                            # Create the new with node
                            return updated_node.with_changes(items=items)

                return updated_node

        transformer = MethodBodyTransformer(self)
        new_body = method_node.body.visit(transformer)

        # Create the new test function with appropriate decorators
        return cst.FunctionDef(
            name=method_node.name,
            params=new_params,
            body=new_body,
            decorators=decorators or [],
            returns=None,
            leading_lines=[cst.EmptyLine()],  # Add empty line before function
            asynchronous=None  # Use None for synchronous functions
        )

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        """
        Final transformations on the module.

        This method is called after all other nodes have been visited.
        It adds fixtures and transformed test functions to the module,
        and ensures pytest is imported.

        Args:
            original_node: The original Module node
            updated_node: The updated Module node from child transformations

        Returns:
            The final transformed Module node
        """
        # Add pytest import if needed
        body = list(updated_node.body)
        imports_to_add = []

        if not self.has_found_pytest:
            imports_to_add.append(
                cst.SimpleStatementLine(
                    body=[
                        cst.Import(
                            names=[cst.ImportAlias(name=cst.Name("pytest"))]
                        )
                    ]
                )
            )

        # Add pytest-mock import comment if needed
        if self.needs_pytest_mock:
            imports_to_add.append(
                cst.SimpleStatementLine(
                    body=[
                        cst.Expr(
                            value=cst.SimpleString('"TODO: Install pytest-mock package if not already installed (pip install pytest-mock)"')
                        )
                    ]
                )
            )

        # Add migration note
        body = imports_to_add + body

        # Add fixtures
        if self.fixtures_to_add:
            body.extend(self.fixtures_to_add)

        # Add test functions
        if self.test_functions_to_add:
            body.extend(self.test_functions_to_add)

        # Return updated module
        return updated_node.with_changes(
            body=body
        )

    def leave_Import(self, original_node: cst.Import, updated_node: cst.Import) -> cst.Import:
        # Check for pytest import
        for alias in original_node.names:
            if isinstance(alias.name, cst.Name) and alias.name.value == "pytest":
                self.has_found_pytest = True
                break
        return updated_node

    def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.ImportFrom:
        # Check if this is a pytest import
        if (
            hasattr(original_node.module, "value") and
            original_node.module.value == "pytest"
        ):
            self.has_found_pytest = True
        return updated_node

    def visit_ClassDef(self, node: cst.ClassDef) -> Optional[bool]:
        """
        Process TestCase classes.

        This method identifies TestCase classes and prepares their transformation.

        Args:
            node: The ClassDef node being visited

        Returns:
            A boolean indicating whether to visit child nodes
        """
        # Check if we've hit one of our TestCase classes
        class_name = node.name.value
        if class_name in self.class_names:
            # Mark this as a test class so we handle it specially
            self.is_test_class = True

            # Get the class data from the visitor
            self.current_class_idx = self.class_names[class_name]
            class_data = self.test_classes[self.current_class_idx]

            # Create fixtures from setup/teardown methods
            if class_data["setup"] or class_data["teardown"]:
                fixture = self._create_fixture_from_setup_teardown(
                    class_name,
                    class_data["setup"],
                    class_data["teardown"]
                )
                self.fixtures_to_add.append(fixture)

            # Process test methods
            if "tests" in class_data:
                for method in class_data["tests"]:
                    # Create a test function from each test method
                    fixture_name = f"{class_name.lower()}_fixture"
                    test_func = self._convert_test_method(method, [fixture_name])
                    self.test_functions_to_add.append(test_func)

            # Check if we need to add the pytest-mock fixture
            if class_data.get("needs_pytest_mock", False):
                self.needs_pytest_mock = True

            # Skip further processing - we've extracted the relevant parts
            # and will create test functions outside the class
            return False

        # Process other classes normally
        return True

    def leave_ClassDef(self, original_node: cst.ClassDef, updated_node: cst.ClassDef) -> cst.CSTNode:
        # No need to transform here since we're handling TestCase classes in visit_ClassDef
        return updated_node


def add_migration_note(module: cst.Module, original_file: str) -> cst.Module:
    """
    Add a migration note comment to the top of the file.
    """
    comment_lines = [
        f"# Pytest version of {os.path.basename(original_file)}",
        "#",
        "# This file was migrated from unittest.TestCase style to pytest style.",
        "# Manual adjustments may still be needed for:",
        "# - Complex assertions",
        "# - Mock implementations",
        "# - Test parameterization",
        "# - Dependency injection",
    ]

    # Create empty lines with comments
    comment_empty_lines = []
    for line in comment_lines:
        comment_empty_lines.append(
            cst.EmptyLine(
                indent=True,
                comment=cst.Comment(value=line),
                newline=cst.Newline()
            )
        )

    # Add an extra newline after comments
    comment_empty_lines.append(cst.EmptyLine())

    # Create a new module with the comments added
    return module.with_changes(header=comment_empty_lines)


def migrate_test_file(file_path: str, output_path: Optional[str] = None) -> None:
    """
    Migrate a unittest.TestCase test file to pytest style using LibCST.

    Args:
        file_path: Path to the test file to migrate
        output_path: Path to write the migrated file to, or None for dry run
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Test file '{file_path}' not found")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source_code = f.read()
    except Exception as e:
        raise IOError(f"Failed to read file '{file_path}': {e}")

    try:
        # Parse the source code
        wrapper = MetadataWrapper(
            cst.parse_module(source_code),
            cache=True,
        )

        # Gather information about the test file
        visitor = TestCaseVisitor()
        wrapper.visit(visitor)

        # Log the test classes found
        test_class_names = [cls["name"] for cls in visitor.test_classes]
        if not test_class_names:
            logger.warning(f"No TestCase classes found in {file_path}")
        else:
            logger.info(f"Found TestCase classes: {', '.join(test_class_names)}")

            # Count test methods
            test_method_count = sum(len(cls["tests"]) for cls in visitor.test_classes)
            logger.info(f"Found {test_method_count} test methods")

            # Check for empty test classes
            for cls in visitor.test_classes:
                if not cls["tests"]:
                    logger.warning(f"No test methods found in TestCase class {cls['name']}")

        # Transform the code to pytest style
        transformer = UnittestToTestReplacer(
            visitor.test_classes, visitor.imports, visitor.import_froms
        )
        transformed_tree = wrapper.module.visit(transformer)

        # Add migration note
        transformed_tree = add_migration_note(transformed_tree, file_path)

        # Generate the transformed code
        transformed_code = transformed_tree.code

        # Write output file
        if output_path:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(transformed_code)
                logger.info(f"Migrated test file written to: {output_path}")

                # Validate the migrated file
                validate_migration(file_path, output_path)
            except Exception as e:
                raise IOError(f"Failed to write to '{output_path}': {e}")
        else:
            logger.info("=== Migrated content (dry run) ===")
            print(transformed_code)
            logger.info("=================================")

        logger.info("Migration completed successfully")

    except cst.ParserSyntaxError as e:
        logger.error(f"Syntax error in source file: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during migration: {e}")
        if logger.level <= logging.DEBUG:
            import traceback
            traceback.print_exc()
        raise


def validate_migration(original_file: str, migrated_file: str) -> None:
    """
    Perform basic validation checks on the migrated file.

    Args:
        original_file: Path to the original unittest test file
        migrated_file: Path to the migrated pytest test file
    """
    try:
        # Read both files
        with open(original_file, "r", encoding="utf-8") as f:
            original_content = f.read()
        with open(migrated_file, "r", encoding="utf-8") as f:
            migrated_content = f.read()

        # Check if pytest is imported in the migrated file
        if "import pytest" not in migrated_content:
            logger.warning("Migrated file does not import pytest")

        # Check if unittest is still imported in the migrated file
        if "import unittest" in migrated_content:
            logger.warning("Migrated file still imports unittest - may need manual cleanup")

        # Count the number of test functions
        original_test_count = len(re.findall(r"def test_\w+\(", original_content))
        migrated_test_count = len(re.findall(r"def test_\w+\(", migrated_content))
        if migrated_test_count < original_test_count:
            logger.warning(f"Possible test loss: Original had {original_test_count} tests, migrated has {migrated_test_count}")
        else:
            logger.info(f"Test function count: {migrated_test_count}")

        # Check if any assert statements were successfully converted
        if "assert " not in migrated_content:
            logger.warning("No pytest assertions found in migrated file")

        # Check for TODO comments
        todos = migrated_content.count("TODO:")
        if todos > 0:
            logger.warning(f"Found {todos} TODO comments in migrated file that need manual attention")

            # Extract and show the first few TODOs with line numbers
            max_todos_to_show = min(5, todos)
            todo_lines = []

            for i, line in enumerate(migrated_content.splitlines()):
                if "TODO:" in line:
                    todo_lines.append((i + 1, line.strip()))
                    if len(todo_lines) >= max_todos_to_show:
                        break

            logger.warning("Sample TODOs with line numbers:")
            for line_num, todo in todo_lines:
                logger.warning(f"  Line {line_num}: {todo}")

            if todos > max_todos_to_show:
                logger.warning(f"  ... and {todos - max_todos_to_show} more TODOs")

        # Check for fixture usage
        fixture_count = len(re.findall(r"@pytest\.fixture", migrated_content))
        if fixture_count > 0:
            logger.info(f"Created {fixture_count} fixtures")

        # Check for common issues
        if "self." in migrated_content:
            logger.warning("Migrated file still contains 'self.' references - may need manual cleanup")

            # Show some examples
            self_lines = []
            for i, line in enumerate(migrated_content.splitlines()):
                if "self." in line:
                    self_lines.append((i + 1, line.strip()))
                    if len(self_lines) >= 3:
                        break

            logger.warning("Sample 'self.' references with line numbers:")
            for line_num, line in self_lines:
                logger.warning(f"  Line {line_num}: {line}")

        if "TestCase" in migrated_content and not "# unittest.TestCase" in migrated_content:
            logger.warning("Migrated file still contains TestCase references - may need manual cleanup")

        # Check if the original file has any mock patches
        if any(term in original_content for term in ["@patch", "@mock.patch", "@unittest.mock.patch"]):
            if "mocker" in migrated_content:
                logger.info("Found mock patches - converted to use pytest-mock fixture")
            else:
                logger.warning("Found mock patches in original file but no sign of pytest-mock conversion - may need manual conversion")

        # Give a summary evaluation
        if todos == 0 and "self." not in migrated_content and "TestCase" not in migrated_content:
            logger.info("✅ Migration looks complete - ready for testing!")
        elif todos <= 3 and not ("self." in migrated_content and "TestCase" in migrated_content):
            logger.info("⚠️ Migration mostly complete - needs minor manual adjustments")
        else:
            logger.warning("⛔ Migration needs significant manual intervention")

    except Exception as e:
        logger.warning(f"Validation failed: {e}")


def migrate_directory(directory_path: str, output_directory: str) -> None:
    """
    Migrate all unittest test files in a directory to pytest style.

    Args:
        directory_path: Path to the directory containing test files
        output_directory: Path to write the migrated files to
    """
    if not os.path.isdir(directory_path):
        raise NotADirectoryError(f"'{directory_path}' is not a directory")

    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)

    # Find all test files in the directory
    test_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.startswith("test_") and file.endswith(".py"):
                test_files.append(os.path.join(root, file))

    if not test_files:
        logger.warning(f"No test files found in {directory_path}")
        return

    logger.info(f"Found {len(test_files)} test files")

    # Migrate each test file
    success_count = 0
    failure_count = 0
    skipped_count = 0

    for file_path in test_files:
        relative_path = os.path.relpath(file_path, directory_path)
        output_path = os.path.join(output_directory, relative_path)

        # Don't overwrite existing files
        if os.path.exists(output_path):
            logger.warning(f"Skipping {relative_path} - output file already exists")
            skipped_count += 1
            continue

        try:
            logger.info(f"Migrating {relative_path}")
            migrate_test_file(file_path, output_path)
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to migrate {relative_path}: {e}")
            failure_count += 1

    logger.info(f"Migration summary: {success_count} succeeded, {failure_count} failed, {skipped_count} skipped")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(description="Test Migration Helper Tool using LibCST")
    parser.add_argument("--file", help="Path to the test file to migrate")
    parser.add_argument("--directory", help="Path to the directory containing test files to migrate")
    parser.add_argument("--output", help="Output path for the migrated file or directory")
    parser.add_argument("--dry-run", action="store_true", help="Only print file, don't write")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    parser.add_argument("--force", "-f", action="store_true", help="Overwrite existing files")

    return parser.parse_args()


def main() -> None:
    """Main function."""
    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    if not args.file and not args.directory:
        logger.error("Either --file or --directory must be specified")
        sys.exit(1)

    if args.file and args.directory:
        logger.error("Cannot specify both --file and --directory")
        sys.exit(1)

    try:
        if args.file:
            logger.info(f"Migrating test file: {args.file}")
            migrate_test_file(
                args.file,
                None if args.dry_run else args.output
            )
        elif args.directory:
            if not args.output and not args.dry_run:
                logger.error("Output directory must be specified when migrating a directory")
                sys.exit(1)

            logger.info(f"Migrating test files in directory: {args.directory}")
            if args.dry_run:
                # In dry-run mode, just list the files that would be migrated
                for root, _, files in os.walk(args.directory):
                    for file in files:
                        if file.startswith("test_") and file.endswith(".py"):
                            logger.info(f"Would migrate: {os.path.join(root, file)}")
            else:
                migrate_directory(args.directory, args.output)

    except FileNotFoundError as e:
        logger.error(f"{e}")
        sys.exit(1)
    except NotADirectoryError as e:
        logger.error(f"{e}")
        sys.exit(1)
    except IOError as e:
        logger.error(f"{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error during migration: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()