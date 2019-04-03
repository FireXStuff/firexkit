import unittest
from firexkit.result import get_results, RETURN_KEYS_KEY


class AsyncResultMock:
    def __init__(self, result={}, successful=True, children=[]):
        self._success = successful
        self._children = children
        self._result = result

    @property
    def result(self):
        return self._result

    def _get_children(self):
        return self._children

    @property
    def children(self):
        return self._get_children()

    def successful(self):
        return self._success


def _get_children_asserts():
    raise AssertionError('Raising a fake assertion for the unit-test')


class GetResultsTests(unittest.TestCase):
    def test_plain_case(self):
        result = {'a': 1, 'b': 2}
        r = AsyncResultMock(result=result)
        self.assertDictEqual(get_results(r, return_keys_only=False), result)
        self.assertDictEqual(get_results(r), {})

    def test_empty_case(self):
        result = {}
        r = AsyncResultMock(result=result)
        self.assertEqual(get_results(r), result)

    def test_none_case(self):
        result = None
        r = AsyncResultMock(result)
        self.assertEqual(get_results(r, return_keys_only=False), {})
        self.assertDictEqual(get_results(r), {})

    def test_unsucessful_result(self):
        result = {'a': 1, 'b': 2}
        r = AsyncResultMock(result=result, successful=False)
        self.assertDictEqual(get_results(r, return_keys_only=False), {})
        self.assertDictEqual(get_results(r), {})

    def test_assertion(self):
        result = {'a': 1, 'b': 2}
        r = AsyncResultMock(result=result)
        # monkey patch
        r._get_children = _get_children_asserts
        self.assertDictEqual(get_results(r, return_keys_only=False), result)
        self.assertDictEqual(get_results(r), {})

    def test_result_with_children(self):
        result = {'a': 1, 'b': 2}
        c1_result = {'c': 3, 'd': 4, 'a': 5}
        c2_result = {'e': 6, 'c': 7}
        c1 = AsyncResultMock(result=c1_result)
        c2 = AsyncResultMock(result=c2_result)

        r = AsyncResultMock(result=result, children=[c1, c2])

        with self.subTest('Merging results from children'):
            expected = result.copy()
            expected.update(c1_result)
            expected.update(c2_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True),expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('No extraction from children'):
            self.assertDictEqual(get_results(r, return_keys_only=False,), result)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})


        with self.subTest('Order of children does matter'):
            r = AsyncResultMock(result=result, children=[c2, c1])
            expected = result.copy()
            expected.update(c2_result)
            expected.update(c1_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

    def test_result_with_children_with_unsuccessful(self):
        result = {'a': 1, 'b': 2}
        c1_result = {'c': 3, 'd': 4, 'a': 5}
        c2_result = {'e': 6, 'c': 7}
        c1 = AsyncResultMock(result=c1_result)
        c2 = AsyncResultMock(result=c2_result)

        with self.subTest('Parent not successful'):
            r = AsyncResultMock(result=result, children=[c1, c2], successful=False)
            expected = c1_result.copy()
            expected.update(c2_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
        self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('Child not successful'):
            c3 = AsyncResultMock(result=c2_result, successful=False)
            r = AsyncResultMock(result=result, children=[c1, c3])
            expected = result.copy()
            expected.update(c1_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('Child asserts'):
            c4 = AsyncResultMock(result=c2_result)
            c4._get_children = _get_children_asserts
            r = AsyncResultMock(result=result, children=[c1, c4])
            expected = result.copy()
            expected.update(c1_result)
            expected.update(c2_result)
            self.assertDictEqual(get_results(r, return_keys_only=False, merge_children_results=True), expected)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

    def test_return_keys(self):
        with self.subTest('return_keys'):
            return_keys = {RETURN_KEYS_KEY: ('a', 'b', 'c', 'd')}
            values = {'a': 1, 'b': 2, 'c': 3, 'd': 4}
            result = values.copy()
            result.update(**return_keys)
            r = AsyncResultMock(result=result)

            with self.subTest('string'):
                self.assertTupleEqual(get_results(r, return_keys='a'), (1, ))
            with self.subTest('tuple of one'):
                self.assertTupleEqual(get_results(r, return_keys=('a',)), (1, ))
            with self.subTest('tuple of two'):
                self.assertTupleEqual(get_results(r, return_keys=('b', 'd')), (2, 4))
            with self.subTest('None'):
                self.assertDictEqual(get_results(r, return_keys=None), values)
            with self.subTest('Non-existent key'):
                self.assertTupleEqual(get_results(r, return_keys=('b', 'z')), (2, None))

    def test_extract_task_returns_only(self):
        with self.subTest('plain case'):
            return_keys = ('a', 'b')
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: return_keys}
            r = AsyncResultMock(result=result)
            self.assertDictEqual(get_results(r, merge_children_results=True),
                                 {k:result[k] for k in return_keys})

        with self.subTest('return keys is None'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: None}
            r = AsyncResultMock(result=result)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('some of the return keys are non-existent'):
            result = {'a': 1, 'b': 2, 'c': 3, RETURN_KEYS_KEY: ('a', 'd')}
            r = AsyncResultMock(result=result)
            self.assertDictEqual(get_results(r, merge_children_results=True), {})

        with self.subTest('child has a return key'):
            result = {'a': 1, 'b': 2}
            c1_result = {'c': 3, 'd': 4, 'a': 5, RETURN_KEYS_KEY: ('d',)}
            c2_result = {'e': 6, 'c': 7}
            c1 = AsyncResultMock(result=c1_result)
            c2 = AsyncResultMock(result=c2_result)
            r = AsyncResultMock(result=result, children=[c1, c2])
            self.assertDictEqual(get_results(r, merge_children_results=True), {'d': 4})

        with self.subTest('parent and child have return keys'):
            return_keys = ('a', )
            result = {'a': 1, 'b': 2, RETURN_KEYS_KEY: return_keys}
            c1_result = {'c': 3, 'd': 4, 'a': 5, RETURN_KEYS_KEY: ('d',)}
            c2_result = {'e': 6, 'c': 7}
            c1 = AsyncResultMock(result=c1_result)
            c2 = AsyncResultMock(result=c2_result)
            r = AsyncResultMock(result=result, children=[c1, c2])
            self.assertDictEqual(get_results(r, merge_children_results=True), {'a': 1, 'd': 4})

        with self.subTest('parent and child have empty return keys'):
            result = {'a': 1, 'b': 2, RETURN_KEYS_KEY: tuple()}
            c1_result = {'c': 3, 'd': 4, 'a': 5, RETURN_KEYS_KEY: ('d',)}
            c2_result = {'e': 6, 'c': 7, RETURN_KEYS_KEY: None}
            c1 = AsyncResultMock(result=c1_result)
            c2 = AsyncResultMock(result=c2_result)
            r = AsyncResultMock(result=result, children=[c1, c2])
            self.assertDictEqual(get_results(r, merge_children_results=True), {'d': 4})
