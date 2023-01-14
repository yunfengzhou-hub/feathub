from typing import List

from _pytest.unittest import TestCaseFunction


def _is_class_contains_method(class_instance, method_name):
    method_list = [func for func in dir(class_instance) if callable(getattr(class_instance, func))]
    return method_name in method_list


def _get_class_with_method(classes, method_name):
    for class_instance in classes:
        if _is_class_contains_method(class_instance, method_name):
            return class_instance


def _is_supported_test_case(test_class, method_name):
    class_with_method = _get_class_with_method(test_class.__bases__, method_name)
    # print(str(test_class.__name__) + " " + str(class_with_method) + " " + str(class_with_method.get_unsupported_test_cases()))
    if class_with_method is not None:
        print(class_with_method.get_unsupported_classes())
    return class_with_method is None or test_class.__name__ not in class_with_method.get_unsupported_test_cases()


def pytest_collection_modifyitems(session, config, items: List[TestCaseFunction]):
    """ called after collection has been performed, may filter or re-order
    the items in-place."""

    print("pytest_collection_modifyitems is invoked")

    original_items = items.copy()
    items.clear()

    for item in original_items:
        if _is_supported_test_case(item.cls, item.name):
            items.append(item)
    #     print(item.cls)
    #     print(item.cls.__bases__)
    #     print(item.cls.__base__)
    #     print(item.cls.__base__.get_unsupported_test_cases())
    #     print(item.name)
    #
    # class_instance = items[0].cls.__bases__[0]
    # method_list = [func for func in dir(class_instance) if callable(getattr(class_instance, func))]
    # print(method_list)

    # found_only_marker = False
    # for item in items.copy():
    #     if item.get_marker('only'):
    #         if not found_only_marker:
    #             items.clear()
    #             found_only_marker = True
    #         items.append(item)