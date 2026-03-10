
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email 
ON users (email);

-- Roles Indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_roles_name 
ON roles (name);

-- User Login Details Indexes
CREATE INDEX IF NOT EXISTS idx_user_login_details_user_id 
ON user_login_details (user_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_login_details_jwt_token 
ON user_login_details (jwt_token);