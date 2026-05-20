index_ascii_pyx = custom_target(
  '_index_ascii_pyx',
  input: '_index_ascii.py',
  output: '_index_ascii.pyx',
  command: [
    py, '-c',
    'import shutil, sys; shutil.copyfile(sys.argv[1], sys.argv[2])',
    '@INPUT@', '@OUTPUT@'
  ],
  build_by_default: true,
)

py.extension_module(
  '_index_ascii',
  index_ascii_pyx,
  subdir: 'metro_eval/cli/metro2hdf',
  install: true,
)
