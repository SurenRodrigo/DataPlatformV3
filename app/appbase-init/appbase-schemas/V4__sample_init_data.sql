-- Insert sample data into users table
INSERT INTO users (email, password_hash, first_name, last_name, is_active, last_login)
VALUES
    ('admin@appbase.com', 'hash12345$2b$10$NwhL5Xu6PcnwsmmokWMYge1Sk1ac8g1ESutiQtLrmL7ZVp3zzEimK', 'Suren', 'Admin', TRUE, '2024-01-01 10:00:00'),
    ('manager@appbase.com', 'hash12345$2b$10$NwhL5Xu6PcnwsmmokWMYge1Sk1ac8g1ESutiQtLrmL7ZVp3zzEimK', 'Dinuka', 'Manager', TRUE, '2024-01-02 11:00:00'),
    ('user@appbase.com', 'hash12345$2b$10$NwhL5Xu6PcnwsmmokWMYge1Sk1ac8g1ESutiQtLrmL7ZVp3zzEimK', 'Kamal', 'User', TRUE, '2024-01-02 11:00:00');
-- Insert sample data into roles table
INSERT INTO roles (name, description, is_system_role)
VALUES
    ('AppBaseAdmin', 'Administrator role with full permissions', TRUE),
    ('Manager', 'Manager role with limited permissions', FALSE),
    ('User', 'General user role', FALSE);


-- Insert sample data into user_roles table
INSERT INTO user_roles (user_id, role_id)
VALUES
    (1, 1),
    (1, 2),
    (1, 3),
    (2, 2),
    (2, 3),
    (3, 3);