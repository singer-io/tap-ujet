.DEFAULT_GOAL := test

test:
	pylint tap_ujet --disable missing-docstring,missing-class-docstring,missing-module-docstring,missing-function-docstring,too-many-instance-attributes,trailing-whitespace,bare-except,logging-format-interpolation,too-many-arguments,too-many-locals,protected-access,unused-argument,unused-argument,too-many-statements,unused-import,dangerous-default-value,useless-object-inheritance,inconsistent-return-statements,unused-variable,no-else-raise,too-many-format-args,too-many-branches
	nosetests
