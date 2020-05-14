CREATE DATABASE pgc4d;

-- Default test user (hostssl + md5)

SET password_encryption = 'md5';
CREATE USER pgc4d PASSWORD 'pgc4d_pass';
GRANT ALL ON DATABASE pgc4d TO pgc4d;

-- Users for testing other connection methods

SET password_encryption = 'md5';
CREATE USER pgc4d_local PASSWORD 'pgc4d_local_pass';
CREATE USER pgc4d_hostssl PASSWORD 'pgc4d_hostssl_pass';
CREATE USER pgc4d_hostnossl PASSWORD 'pgc4d_hostnossl_pass';

GRANT ALL ON DATABASE pgc4d TO pgc4d_local;
GRANT ALL ON DATABASE pgc4d TO pgc4d_hostssl;
GRANT ALL ON DATABASE pgc4d TO pgc4d_hostnossl;

-- Users for testing other auth mechanisms

CREATE USER pgc4d_trust PASSWORD NULL;

SET password_encryption = 'md5';
CREATE USER pgc4d_clear PASSWORD 'pgc4d_clear_pass';

SET password_encryption = 'md5';
CREATE USER pgc4d_md5 PASSWORD 'pgc4d_md5_pass';

SET password_encryption = 'scram-sha-256';
CREATE USER pgc4d_scram PASSWORD 'pgc4d_scram_pass';

GRANT ALL ON DATABASE pgc4d TO pgc4d_trust;
GRANT ALL ON DATABASE pgc4d TO pgc4d_clear;
GRANT ALL ON DATABASE pgc4d TO pgc4d_md5;
GRANT ALL ON DATABASE pgc4d TO pgc4d_scram;
