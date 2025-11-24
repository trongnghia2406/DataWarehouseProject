/*
 Navicat Premium Dump SQL

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 80300 (8.3.0)
 Source Host           : localhost:3306
 Source Schema         : db_control

 Target Server Type    : MySQL
 Target Server Version : 80300 (8.3.0)
 File Encoding         : 65001

 Date: 23/11/2025 18:54:46
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for config
-- ----------------------------
DROP TABLE IF EXISTS `config`;
CREATE TABLE `config`  (
  `ID` int NOT NULL AUTO_INCREMENT,
  `TEN` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `URL` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_PRODUCT_CONTAINER` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `PRODUCT_CONTAINER` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `DATE_CONFIG` date NULL DEFAULT NULL,
  `THE_TEN_SP` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_LINK` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_LINK_ANH` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_GIA_CU` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_GIA_MOI` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_KICH_THUOC_MAN_HINH` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_RAM` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_BO_NHO` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_GIAM_GIA_SMEMBER` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_GIAM_GIA_SSTUDENT` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_GIAM_GIA_PHAN_TRAM` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_COUPON` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_QUA_TANG` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_DANH_GIA` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_DA_BAN` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `THE_BTN_SHOW_MORE` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  PRIMARY KEY (`ID`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of config
-- ----------------------------
INSERT INTO `config` VALUES (1, 'CELLPHONES', 'https://cellphones.com.vn/mobile.html', 'div.product-info-container.product-item', 'div.product-info-container.product-item', '2025-11-04', '.product__name h3', 'a.product__link', '.product__image img', '.product__price--through', '.product__price--show', '.product__badge p', '.product__badge p', '.product__badge p', '.block-smem-price', '.block-smem-price.edu', '.product__box-rating', '.coupon-price', '', '', '', 'a.btn-show-more');
INSERT INTO `config` VALUES (2, 'TGDD', 'https://www.thegioididong.com/dtdd', 'li.item', 'li.item', '2025-11-04', 'h3', 'a.main-contain', '.item-img img', '.price-old', 'strong.price', '.item-compare span', '.prods-group li.act', '.prods-group li.act', '', '', '.percent', '', '.item-gift', '.rating_Compare .vote-txt b', '.rating_Compare span', 'strong.see-more-btn');

-- ----------------------------
-- Table structure for crawl_log
-- ----------------------------
DROP TABLE IF EXISTS `crawl_log`;
CREATE TABLE `crawl_log`  (
  `ID` int NOT NULL AUTO_INCREMENT,
  `ID_CONFIG` int NULL DEFAULT NULL,
  `RUN_DATE` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `STATUS` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `FILE_PATH` varchar(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `SITE_NAME` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `ROWS_AFFECTED` int NULL DEFAULT 0,
  `ERROR_MESSAGE` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  PRIMARY KEY (`ID`) USING BTREE,
  INDEX `FK_CRAWL_CONFIG`(`ID_CONFIG` ASC) USING BTREE,
  CONSTRAINT `FK_CRAWL_CONFIG` FOREIGN KEY (`ID_CONFIG`) REFERENCES `config` (`ID`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE = InnoDB AUTO_INCREMENT = 31 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of crawl_log
-- ----------------------------
INSERT INTO `crawl_log` VALUES (1, 1, '2025-11-07 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_07.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (2, 2, '2025-11-07 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_07.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (3, 1, '2025-11-08 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_08.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (4, 2, '2025-11-08 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_08.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (5, 1, '2025-11-09 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_09.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (6, 2, '2025-11-09 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_09.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (7, 1, '2025-11-10 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_10.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (8, 2, '2025-11-10 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_10.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (9, 1, '2025-11-11 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_11.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (10, 2, '2025-11-11 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_11.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (11, 1, '2025-11-12 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_12.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (12, 2, '2025-11-12 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_12.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (13, 1, '2025-11-14 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_14.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (14, 2, '2025-11-14 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_14.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (15, 1, '2025-11-15 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_15.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (16, 2, '2025-11-15 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_15.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (17, 1, '2025-11-16 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_16.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (18, 2, '2025-11-16 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_16.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (19, 1, '2025-11-17 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_17.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (20, 2, '2025-11-17 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_17.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (21, 1, '2025-11-18 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_18.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (22, 2, '2025-11-18 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_18.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (23, 1, '2025-11-19 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_19.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (24, 2, '2025-11-19 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_19.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (25, 1, '2025-11-21 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_21.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (26, 2, '2025-11-21 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_21.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (27, 1, '2025-11-22 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_22.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (28, 2, '2025-11-22 21:00:00', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_22.csv', 'TGDD', 120, NULL);
INSERT INTO `crawl_log` VALUES (29, 1, '2025-11-23 16:03:36', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_23.csv', 'CELLPHONES', 120, NULL);
INSERT INTO `crawl_log` VALUES (30, 2, '2025-11-23 16:04:04', 'SUCCESS', 'D:\\Project_DW\\products_raw_2025_11_23.csv', 'TGDD', 119, NULL);

-- ----------------------------
-- Table structure for process
-- ----------------------------
DROP TABLE IF EXISTS `process`;
CREATE TABLE `process`  (
  `ID` int NOT NULL AUTO_INCREMENT,
  `TEN_PROCESS` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `SOURCE_TABLE` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `TARGET_TABLE` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `IS_ACTIVE` tinyint(1) NULL DEFAULT 1,
  PRIMARY KEY (`ID`) USING BTREE
) ENGINE = MyISAM AUTO_INCREMENT = 2 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of process
-- ----------------------------
INSERT INTO `process` VALUES (1, 'Transform_Process', 'PRODUCTS_GENERAL', 'PRODUCTS_TRANSFORM', 1);

-- ----------------------------
-- Table structure for process_log
-- ----------------------------
DROP TABLE IF EXISTS `process_log`;
CREATE TABLE `process_log`  (
  `ID` int NOT NULL AUTO_INCREMENT,
  `ID_PROCESS` int NULL DEFAULT NULL,
  `START_TIME` datetime NULL DEFAULT CURRENT_TIMESTAMP,
  `END_TIME` datetime NULL DEFAULT NULL,
  `STATUS` varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  `MESSAGE` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL,
  PRIMARY KEY (`ID`) USING BTREE,
  INDEX `FK_PROCESSLOG_PROCESS`(`ID_PROCESS`) USING BTREE
) ENGINE = MyISAM AUTO_INCREMENT = 5 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of process_log
-- ----------------------------
INSERT INTO `process_log` VALUES (1, NULL, '2025-11-23 16:02:29', NULL, 'LOAD_STAGING_WARN', '{\"rows\": 0, \"details\": {\"error\": \"CSV rỗng\"}}');
INSERT INTO `process_log` VALUES (2, NULL, '2025-11-23 16:04:49', NULL, 'LOAD_STAGING_SUCCESS', '{\"rows\": 239, \"details\": {\"source\": \"products_raw_2025_11_23.csv\", \"insert_cols\": [\"ID\", \"TEN\", \"LINK\", \"LINK_ANH\", \"GIA_CU\", \"GIA_MOI\", \"KICH_THUOC_MAN_HINH\", \"RAM\", \"BO_NHO\", \"NGAY\", \"ID_CONFIG\"]}}');
INSERT INTO `process_log` VALUES (3, 1, '2025-11-23 18:49:26', '2025-11-23 18:49:26', 'FAILED_SCD_UPDATE', 'SCD Type 2 completed successfully. Rows Processed: 0. New Rows Inserted: 0. Old Rows Expired (Updated): 0.');
INSERT INTO `process_log` VALUES (4, 1, '2025-11-23 18:51:37', '2025-11-23 18:51:37', 'SUCCESS', 'SCD Type 2 completed successfully. Rows Processed: 239. New Rows Inserted: 239. Old Rows Expired (Updated): 0.');

-- ----------------------------
-- Table structure for sql_commands
-- ----------------------------
DROP TABLE IF EXISTS `sql_commands`;
CREATE TABLE `sql_commands`  (
  `ID` int NOT NULL AUTO_INCREMENT,
  `COMMAND_NAME` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `SQL_QUERY` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `DESCRIPTION` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL DEFAULT NULL,
  PRIMARY KEY (`ID`) USING BTREE,
  UNIQUE INDEX `COMMAND_NAME`(`COMMAND_NAME` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 11 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of sql_commands
-- ----------------------------
INSERT INTO `sql_commands` VALUES (1, 'SP_ETL_CLEAN_DATA', 'DELIMITER $$\nCREATE PROCEDURE SP_ETL_Clean_Data ()\nBEGIN\nTRUNCATE TABLE PRODUCTS_TRANSFORM;\nINSERT INTO PRODUCTS_TRANSFORM (\nID, TEN, LINK, LINK_ANH, GIA_CU, GIA_MOI,\nKICH_THUOC_MAN_HINH, RAM, BO_NHO, SK_DATE, NGAY, ID_CONFIG\n)\nWITH TransformedSourceData AS (\nSELECT\nG.ID,\nIFNULL(NULLIF(G.TEN, \'\'), \'Unknown\') AS TEN,\nIFNULL(NULLIF(G.LINK, \'\'), \'Unknown\') AS LINK,\nIFNULL(NULLIF(G.LINK_ANH, \'\'), \'Unknown\') AS LINK_ANH,\nIF(G.GIA_CU IS NULL OR G.GIA_CU = \'\' OR G.GIA_CU = \'-1\', -1,\nCOALESCE(CAST(REPLACE(REPLACE(REPLACE(REPLACE(G.GIA_CU, \'.\', \'\'), \',\', \'\'), \'₫\', \'\'), \'đ\', \'\') AS DECIMAL(18,2)), -1)\n) AS GIA_CU,\nIF(G.GIA_MOI IS NULL OR G.GIA_MOI = \'\' OR G.GIA_MOI = \'-1\', -1,\nCOALESCE(CAST(REPLACE(REPLACE(REPLACE(REPLACE(G.GIA_MOI, \'.\', \'\'), \',\', \'\'), \'₫\', \'\'), \'đ\', \'\') AS DECIMAL(18,2)), -1)\n) AS GIA_MOI,\nIF(G.KICH_THUOC_MAN_HINH IS NULL OR G.KICH_THUOC_MAN_HINH = \'\', -1,\nCOALESCE(CAST(REGEXP_SUBSTR(G.KICH_THUOC_MAN_HINH, \'[0-9]*\\.?[0-9]+\') AS DECIMAL(4,2)), -1)\n) AS KICH_THUOC_MAN_HINH,\nIF(G.RAM IS NULL OR G.RAM = \'\', -1,\nCOALESCE(CAST(REPLACE(REPLACE(G.RAM, \'GB\', \'\'), \' \', \'\') AS SIGNED), -1)) AS RAM,\nIF(G.BO_NHO IS NULL OR G.BO_NHO = \'\', -1,\nCOALESCE(CAST(REPLACE(REPLACE(G.BO_NHO, \'GB\', \'\'), \' \', \'\') AS SIGNED), -1)\n) AS BO_NHO,\nIFNULL(dd.DATE_SK, 0) AS SK_DATE,\nG.NGAY, ID_CONFIG\nFROM PRODUCTS_GENERAL G\nLEFT JOIN db_staging.DIM_DATE dd ON DATE(G.NGAY) = dd.FULL_DATE\nWHERE LENGTH(IFNULL(G.TEN,\'\')) + LENGTH(IFNULL(G.LINK,\'\')) + LENGTH(IFNULL(G.LINK_ANH,\'\')) > 10\n)\nSELECT\ns.ID, s.TEN, s.LINK, s.LINK_ANH, s.GIA_CU, s.GIA_MOI,\ns.KICH_THUOC_MAN_HINH, s.RAM, s.BO_NHO, s.SK_DATE, s.NGAY, s.ID_CONFIG\nFROM TransformedSourceData s;\nEND$$\nDELIMITER ;', 'Làm sạch dữ liệu từ PRODUCTS_GENERAL và nạp toàn bộ vào PRODUCTS_TRANSFORM.');
INSERT INTO `sql_commands` VALUES (2, 'SP_ETL_SCD_UPDATE_PRODUCT', 'DELIMITER $$\nCREATE PROCEDURE SP_ETL_SCD_Update_Product (\nOUT p_RowsInput INT,\nOUT p_RowsInserted INT,\nOUT p_RowsUpdated INT\n)\nBEGIN\nDECLARE CurrentRowsInserted INT DEFAULT 0;\n\nSELECT COUNT(*) INTO p_RowsInput\nFROM PRODUCTS_TRANSFORM;\n\nINSERT INTO PRODUCTS_EXPIRED (\nID, TEN, LINK, LINK_ANH, GIA_CU, GIA_MOI,\nKICH_THUOC_MAN_HINH, RAM, BO_NHO, SK_DATE, NGAY, ID_CONFIG,\nCREATED_AT, UPDATED_AT, EXPIRED_AT\n)\nSELECT\ns.ID, s.TEN, s.LINK, s.LINK_ANH, s.GIA_CU, s.GIA_MOI,\ns.KICH_THUOC_MAN_HINH, s.RAM, s.BO_NHO, s.SK_DATE, s.NGAY, s.ID_CONFIG,\nNOW(), NOW(), \'9999-12-31\'\nFROM PRODUCTS_TRANSFORM s\nLEFT JOIN PRODUCTS_EXPIRED t\nON s.TEN = t.TEN\nAND t.EXPIRED_AT = \'9999-12-31\'\nWHERE t.TEN IS NULL;\n\nSET CurrentRowsInserted = ROW_COUNT();\n\nTRUNCATE TABLE db_staging.EXPIRED_KEYS; \nINSERT INTO db_staging.EXPIRED_KEYS (TEN, LINK)\nSELECT t.TEN, t.LINK\nFROM PRODUCTS_EXPIRED t\nJOIN PRODUCTS_TRANSFORM s\nON t.TEN = s.TEN\nAND t.EXPIRED_AT = \'9999-12-31\'\nWHERE\nt.GIA_CU <> s.GIA_CU OR\nt.GIA_MOI <> s.GIA_MOI OR\nt.KICH_THUOC_MAN_HINH <> s.KICH_THUOC_MAN_HINH OR\nt.RAM <> s.RAM OR\nt.BO_NHO <> s.BO_NHO;\n\nUPDATE PRODUCTS_EXPIRED t\nINNER JOIN db_staging.EXPIRED_KEYS ek ON t.TEN = ek.TEN AND t.LINK = ek.LINK\nSET t.EXPIRED_AT = NOW(), t.UPDATED_AT = NOW()\nWHERE t.EXPIRED_AT = \'9999-12-31\';\n\nSET p_RowsUpdated = ROW_COUNT();\n\nINSERT INTO PRODUCTS_EXPIRED (\nID, TEN, LINK, LINK_ANH, GIA_CU, GIA_MOI,\nKICH_THUOC_MAN_HINH, RAM, BO_NHO, SK_DATE, NGAY, ID_CONFIG,\nCREATED_AT, UPDATED_AT, EXPIRED_AT\n)\nSELECT\ns.ID, s.TEN, s.LINK, s.LINK_ANH, s.GIA_CU, s.GIA_MOI,\ns.KICH_THUOC_MAN_HINH, s.RAM, s.BO_NHO, s.SK_DATE, s.NGAY, s.ID_CONFIG,\nNOW(), NOW(), \'9999-12-31\'\nFROM PRODUCTS_TRANSFORM s\nJOIN db_staging.EXPIRED_KEYS ek ON s.TEN = ek.TEN AND s.LINK = ek.LINK;\n\nSET p_RowsInserted = CurrentRowsInserted + ROW_COUNT();\nEND$$\nDELIMITER ;', 'Thực hiện logic Slowly Changing Dimension Type 2 (SCD Type 2) cho bảng sản phẩm.');
INSERT INTO `sql_commands` VALUES (3, 'SP_ETL_UPDATE_LOG_STATUS', 'DELIMITER $$\nCREATE PROCEDURE SP_ETL_Update_Log_Status (\nIN p_ProcessLogID INT,\nIN p_RowsInput INT,\nIN p_RowsInserted INT,\nIN p_RowsUpdated INT,\nIN p_Status VARCHAR(50)\n)\nBEGIN\nDECLARE log_message TEXT;\n\nSET log_message = CONCAT(\n\'SCD Type 2 completed successfully. \',\n\'Rows Processed: \', p_RowsInput, \'. \',\n\'New Rows Inserted: \', p_RowsInserted, \'. \',\n\'Old Rows Expired (Updated): \', p_RowsUpdated, \'.\'\n);\n\nUPDATE db_control.PROCESS_LOG\nSET\nEND_TIME = NOW(),\nSTATUS = p_Status,\nMESSAGE = log_message\nWHERE ID = p_ProcessLogID;\nEND$$\nDELIMITER ;', 'Cập nhật log vào PROCESS_LOG.');
INSERT INTO `sql_commands` VALUES (4, 'SP_ETL_PRODUCT_SCD_EXEC', 'CALL SP_ETL_Product_SCD(?)', 'Thực thi Stored Procedure ETL và truyền ID của PROCESS_LOG.');
INSERT INTO `sql_commands` VALUES (5, 'COUNT_RUNNING_PROCESS_LOG', 'SELECT COUNT(*) AS running_count FROM PROCESS_LOG WHERE STATUS = \'Running\'', 'Đếm số lượng tiến trình đang ở trạng thái Running trong bảng PROCESS_LOG.');
INSERT INTO `sql_commands` VALUES (6, 'COUNT_RUNNING_ETL_LOG', 'SELECT COUNT(*) AS running_count FROM ETL_LOG WHERE STATUS = \'Running\'', 'Đếm số lượng tiến trình đang ở trạng thái Running trong bảng ETL_LOG.');
INSERT INTO `sql_commands` VALUES (7, 'SELECT_PROCESS_ID', 'SELECT ID FROM PROCESS WHERE TEN_PROCESS = %s', 'Lấy ID của tiến trình từ bảng PROCESS dựa trên tên (sử dụng tham số %s).');
INSERT INTO `sql_commands` VALUES (8, 'INSERT_PROCESS_LOG_RUNNING', 'INSERT INTO PROCESS_LOG (ID_PROCESS, STATUS, MESSAGE) VALUES (%s, %s, \'Process started\')', 'Chèn một dòng mới vào PROCESS_LOG, thiết lập trạng thái Running và thông báo bắt đầu.');
INSERT INTO `sql_commands` VALUES (9, 'SELECT_PROCESS_LOG_WAITING', 'SELECT ID FROM PROCESS_LOG WHERE ID_PROCESS = %s AND STATUS = \'WAITING\' LIMIT 1', '');
INSERT INTO `sql_commands` VALUES (10, 'UPDATE_PROCESS_LOG_RUNNING', 'UPDATE PROCESS_LOG SET STATUS = %s WHERE ID = %s', '');

-- ----------------------------
-- Procedure structure for SP_ETL_Update_Log_Status
-- ----------------------------
DROP PROCEDURE IF EXISTS `SP_ETL_Update_Log_Status`;
delimiter ;;
CREATE DEFINER=`root`@`localhost` PROCEDURE `SP_ETL_Update_Log_Status`(
IN p_ProcessLogID INT,
IN p_RowsInput INT,
IN p_RowsInserted INT,
IN p_RowsUpdated INT,
IN p_Status VARCHAR(50)
)
BEGIN
DECLARE log_message TEXT;

SET log_message = CONCAT(
'SCD Type 2 completed successfully. ',
'Rows Processed: ', p_RowsInput, '. ',
'New Rows Inserted: ', p_RowsInserted, '. ',
'Old Rows Expired (Updated): ', p_RowsUpdated, '.'
);

UPDATE db_control.PROCESS_LOG
SET
END_TIME = NOW(),
STATUS = p_Status,
MESSAGE = log_message
WHERE ID = p_ProcessLogID;
END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
