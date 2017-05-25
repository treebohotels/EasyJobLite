def dummy_function_external(body):
    TestClass.module_function_called = True
    return "YIPPI called: " + str(body)


class TestClass(object):
    module_function_called = False
    class_function_called = False
    class_instance_called = False

    def __call__(self, *args, **kwargs):
        TestClass.class_instance_called = True
        return "YIPPI called: " + str(args[0])

    def dummy_function_in_class(self, body):
        TestClass.class_function_called = True
        return "YIPPI called: " + str(body)
