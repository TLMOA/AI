-- 数据库初始化（deploy.sh 会把 __DB_NAME__/__DB_USER__/__DB_PASS__ 替换掉）
CREATE DATABASE IF NOT EXISTS __DB_NAME__ CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '__DB_USER__'@'localhost' IDENTIFIED BY '__DB_PASS__';
ALTER USER '__DB_USER__'@'localhost' IDENTIFIED BY '__DB_PASS__';
GRANT ALL PRIVILEGES ON __DB_NAME__.* TO '__DB_USER__'@'localhost';
FLUSH PRIVILEGES;

USE __DB_NAME__;

CREATE TABLE IF NOT EXISTS iot_users (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    username       VARCHAR(128) NOT NULL UNIQUE,
    password_hash  VARCHAR(256) NOT NULL,
    is_admin       TINYINT      DEFAULT 0,
    deployment_mode VARCHAR(32) DEFAULT 'public',
    ceph_endpoint  VARCHAR(512) DEFAULT '',
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
