-- create demo table
CREATE TABLE spatial_data(id int, name varchar2(50), geo SDO_GEOMETRY);
-- insert some records
INSERT INTO HR.spatial_data (id, name, geo) VALUES (
       1,
       'Point 1',
       MDSYS.SDO_GEOMETRY(
         2001, -- 二维点类型
         NULL,
         MDSYS.SDO_POINT_TYPE(1, 1, NULL), -- X, Y 坐标为 1
         NULL,
         NULL
           )
   );

INSERT INTO HR.spatial_data (id, name, geo) VALUES (
       2,
       'Line 1',
       MDSYS.SDO_GEOMETRY(
         2002, -- 二维线类型
         NULL,
         NULL,
         MDSYS.SDO_ELEM_INFO_ARRAY(1,2,1), -- 表示一个由两个点组成的线段
         MDSYS.SDO_ORDINATE_ARRAY(1,1, 2,2) -- 线段的起点为 (1,1)，终点为 (2,2)
           )
   );

INSERT INTO HR.spatial_data (id, name, geo) VALUES (
       3,
       'Polygon 1',
       MDSYS.SDO_GEOMETRY(
         2003, -- 二维多边形类型
         NULL,
         NULL,
         MDSYS.SDO_ELEM_INFO_ARRAY(1,1003,1), -- 表示一个由三个点组成的三角形
         MDSYS.SDO_ORDINATE_ARRAY(1,1, 2,2, 3,1) -- 三角形的三个顶点分别为 (1,1)、(2,2) 和 (3,1)
           )
   );
