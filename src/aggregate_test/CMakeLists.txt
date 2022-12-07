set(volume_test_srcs
  simple_volume.cc
  simple_aggregate_buffer.cc
  test_local_volume.cc)
add_executable(test_local_volume ${volume_test_srcs})

target_link_libraries(test_local_volume librados global ${BLKID_LIBRARIES} ${CMAKE_DL_LIBS})
install(TARGETS test_local_volume DESTINATION bin)
