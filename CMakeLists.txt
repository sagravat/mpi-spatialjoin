cmake_minimum_required(VERSION 2.8)

project( RTree_MPI)

#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp")
#SET(CMAKE_CC_FLAGS "${CMAKE_CC_FLAGS} -fopenmp")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lm")

find_package(CGAL QUIET COMPONENTS Core
    PATHS
    /home/sagrava/work/virtual-gis/trunk/spatial/downloads/CGAL-3.8-beta1)
    #/opt/local/hadoop-home/temp/CGAL-3.8)
if ( CGAL_FOUND )
    message("CGAL:")
    include( ${CGAL_USE_FILE} )
 
    include( CGAL_CreateSingleSourceCGALProgram )

    find_library(SI NAMES spatialindex 
        PATHS /home/sagrava/work/virtual-gis/trunk/spatial/local/lib
        )
    find_library(BOOST_SYSTEM NAMES boost_system
        PATHS /home/sagrava/work/virtual-gis/trunk/spatial/local/lib
        )
    find_library(BOOST_SER NAMES boost_serialization
        PATHS /home/sagrava/work/virtual-gis/trunk/spatial/local/lib
        )
    find_library(BOOST_FILE NAMES boost_filesystem
        PATHS /home/sagrava/work/virtual-gis/trunk/spatial/local/lib
        )
    find_library(BOOST_MPI NAMES boost_mpi
        PATHS /home/sagrava/work/virtual-gis/trunk/spatial/local/lib
        )
    include_directories(.)
    ##include_directories(/opt/local/hadoop-home/temp/CGAL-3.8/include)
    #include_directories(/home/sagrava/work/virtual-gis/trunk/spatial/downloads/CGAL-3.8-beta1)
    #include_directories(/home/sagrava/work/virtual-gis/trunk/spatial/local/include/spatialindex)
    #include_directories(/home/sagrava/work/virtual-gis/trunk/spatial/local/include/boost)
    SET(CMAKE_CXX_COMPILER mpicxx)
    #SET(LIBS ${LIBS} ${SI} ${BOOST_SYSTEM} ${BOOST_FILE}
    #    ${BOOST_SER} ${BOOST_MPI})
    set(CMAKE_CXX_FLAGS "-L/home/sagrava/work/virtual-gis/trunk/spatial/local/lib")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lboost_system -lboost_serialization")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lpthread -lspatialindex -lboost_mpi -lboost_filesystem") 
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lmpfr -lgmpxx -lgmp") 
    #set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -lCGAL_Core -lCGAL -lCGAL_ImageIO") 
   
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I/home/sagrava/work/virtual-gis/trunk/spatial/local/include/spatialindex -I/home/sagrava/work/virtual-gis/trunk/spatial/local/include/boost")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I/home/sagrava/work/virtual-gis/trunk/spatial/downloads/CGAL-3.8-beta1/include")  
   
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L/home/sagrava/work/virtual-gis/trunk/spatial/downloads/CGAL-3.8-beta1/lib")   
   
    
    
    
    add_executable(spatialindex_mpi spatialindex_mpi.cpp)
    add_executable(spatialindex_local_mpi spatialindex_local_mpi.cpp)
    add_executable(spatialjoin_mpi spatialjoin_mpi.cpp)
    add_executable(slide_to_tile slide_to_tile.cpp)
    add_executable(push push.cpp)
    add_executable(binpack binpack.cpp)
    #add_executable(boost boost.cpp)
    message("done")
    #target_link_libraries(slide_to_tile ${LIBS})
    
    #SET_TARGET_PROPERTIES(push PROPERTIES LINK_FLAGS "-lboost_system -lboost_serialization -lpthread -lspatialindex -lboost_mp -lboost_filesystem")

    
else()
    message(" CGAL NOT FOUND")
endif()
