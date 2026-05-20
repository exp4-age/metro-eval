project(
  'metro-eval',
  version: '0.0.2',
  meson_version: '>= 1.5.0',
)

py = import('python').find_installation(pure: false)

install_subdir(
  'src/metro_eval',
  install_dir: py.get_install_dir(),
  exclude_directories: ['__pycache__'],
)

cythonize = get_option('cythonize')
build_extensions = false

if cythonize.disabled()
  message('Cython extensions disabled by user option')
else
  build_extensions = add_languages('c', 'cython', required: cythonize)

  if not build_extensions
    message('No suitable C/Cython toolchain found; skipping optional extensions.')
  endif
endif

if build_extensions
  subdir('src/metro_eval/cli/sort_events')
  subdir('src/metro_eval/cli/metro2hdf')
endif
