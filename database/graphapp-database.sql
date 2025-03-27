CREATE DATABASE IF NOT EXISTS graphapp;

USE graphapp;

DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS graphs;

CREATE TABLE graphs
(
    graphid           int not null AUTO_INCREMENT,
    datafilekey       varchar(256) not null,
    visualfilekey     varchar(256),
    PRIMARY KEY (graphid)
);

ALTER TABLE graphs AUTO_INCREMENT = 10001;  -- starting value

CREATE TABLE jobs
(
    jobid             int not null AUTO_INCREMENT,
    graphid           int not null,
    status            varchar(256) not null,
    resultsfilekey    varchar(256),
    PRIMARY KEY (jobid),
    FOREIGN KEY (graphid) REFERENCES graphs(graphid)
);

ALTER TABLE jobs AUTO_INCREMENT = 80001;  -- starting value

--
-- creating user accounts for database access:
--
-- ref: https://dev.mysql.com/doc/refman/8.0/en/create-user.html
--

DROP USER IF EXISTS 'graphapp-read-only';
DROP USER IF EXISTS 'graphapp-read-write';

CREATE USER 'graphapp-read-only' IDENTIFIED BY 'abc123!!';
CREATE USER 'graphapp-read-write' IDENTIFIED BY 'def456!!';

GRANT SELECT, SHOW VIEW ON graphapp.* 
      TO 'graphapp-read-only';
GRANT SELECT, SHOW VIEW, INSERT, UPDATE, DELETE, DROP, CREATE, ALTER ON graphapp.* 
      TO 'graphapp-read-write';
      
FLUSH PRIVILEGES;

--
-- done
--

