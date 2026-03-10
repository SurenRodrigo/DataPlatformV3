-- User Roles Foreign Key Constraints
ALTER TABLE user_roles
ADD CONSTRAINT fk_user_roles_user
FOREIGN KEY (user_id)
REFERENCES users(id)
ON UPDATE CASCADE
ON DELETE CASCADE;

ALTER TABLE user_roles
ADD CONSTRAINT fk_user_roles_role
FOREIGN KEY (role_id)
REFERENCES roles(id)
ON UPDATE CASCADE
ON DELETE CASCADE;


-- User Login Details Foreign Key Constraint
ALTER TABLE user_login_details
ADD CONSTRAINT fk_user_login_details_user
FOREIGN KEY (user_id)
REFERENCES users(id)
ON UPDATE CASCADE
ON DELETE CASCADE;
