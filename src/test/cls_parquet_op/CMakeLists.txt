# ceph_test_cls_parquet_op
add_executable(ceph_test_cls_parquet_op
	  test_cls_parquet_op.cc)
target_link_libraries(ceph_test_cls_parquet_op
  librados
  global
 
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx
  ${UNITTEST_LIBS}
  Arrow::Arrow
  Arrow::Parquet 
  )
install(TARGETS
  ceph_test_cls_parquet_op
  DESTINATION ${CMAKE_INSTALL_BINDIR})
