# ceph_test_cls_parquet_scan
add_executable(ceph_test_cls_parquet_scan 
	  test_cls_parquet_scan.cc)
target_link_libraries(ceph_test_cls_parquet_scan
  librados
  global
  cls_parquet_scan_client 
  ${EXTRALIBS}
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  radostest-cxx
  ${UNITTEST_LIBS}
  Arrow::Arrow
  Arrow::Parquet 
  )
install(TARGETS
  ceph_test_cls_parquet_scan
  DESTINATION ${CMAKE_INSTALL_BINDIR})
