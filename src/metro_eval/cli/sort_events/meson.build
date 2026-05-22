numpy_inc = run_command(
  py,
  ['-c', 'import numpy; print(numpy.get_include())'],
  check: true
).stdout().strip()

py.extension_module(
  'sorting_tdc',
  'sorting_tdc.pyx',
  subdir: 'metro_eval/cli/sort_events',
  c_args: ['-I' + numpy_inc],
  install: true,
)
