set(volume_test_srcs
  ${PROJECT_SOURCE_DIR}/src/test_local_volume.cc
  ${PROJECT_SOURCE_DIR}/src/simple_aggregate_buffer.cc
  ${PROJECT_SOURCE_DIR}/src/simple_volume.cc)
add_executable(test_local_volume ${volume_test_srcs})
