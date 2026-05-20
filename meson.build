project('metro-eval', 'c', 'cython')

py = import('python').find_installation(pure: false)

install_subdir(
  'src/metro_eval',
  install_dir: py.get_install_dir(),
  exclude_directories: ['__pycache__'],
)

subdir('src/metro_eval/cli/sort_events')
subdir('src/metro_eval/cli/metro2hdf')
