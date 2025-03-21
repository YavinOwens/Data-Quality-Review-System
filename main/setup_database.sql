-- Create departments table
CREATE TABLE IF NOT EXISTS departments (
    dept_no VARCHAR(4) PRIMARY KEY,
    dept_name VARCHAR(40) NOT NULL
);

-- Create employees table
CREATE TABLE IF NOT EXISTS employees (
    emp_no INTEGER PRIMARY KEY,
    first_name VARCHAR(14) NOT NULL,
    last_name VARCHAR(16) NOT NULL,
    gender CHAR(1) NOT NULL,
    hire_date DATE NOT NULL
);

-- Create salaries table
CREATE TABLE IF NOT EXISTS salaries (
    emp_no INTEGER,
    salary INTEGER NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    PRIMARY KEY (emp_no, from_date),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no)
);

-- Create dept_emp table
CREATE TABLE IF NOT EXISTS dept_emp (
    emp_no INTEGER,
    dept_no VARCHAR(4),
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    PRIMARY KEY (emp_no, dept_no),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no),
    FOREIGN KEY (dept_no) REFERENCES departments(dept_no)
);

-- Create titles table
CREATE TABLE IF NOT EXISTS titles (
    emp_no INTEGER,
    title VARCHAR(50) NOT NULL,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    PRIMARY KEY (emp_no, title, from_date),
    FOREIGN KEY (emp_no) REFERENCES employees(emp_no)
);

-- Insert sample data into departments
INSERT INTO departments (dept_no, dept_name) VALUES
('d001', 'Marketing'),
('d002', 'Finance'),
('d003', 'Human Resources'),
('d004', 'Production'),
('d005', 'Development'),
('d006', 'Quality Management'),
('d007', 'Sales'),
('d008', 'Research'),
('d009', 'Customer Service');

-- Insert sample data into employees
INSERT INTO employees (emp_no, first_name, last_name, gender, hire_date) VALUES
(10001, 'Georgi', 'Facello', 'M', '1986-06-26'),
(10002, 'Bezalel', 'Simmel', 'F', '1985-11-21'),
(10003, 'Parto', 'Bamford', 'M', '1986-08-28'),
(10004, 'Chirstian', 'Koblick', 'M', '1986-12-01'),
(10005, 'Kyoichi', 'Maliniak', 'M', '1989-09-12');

-- Insert sample data into salaries
INSERT INTO salaries (emp_no, salary, from_date, to_date) VALUES
(10001, 60117, '1986-06-26', '9999-01-01'),
(10002, 65828, '1985-11-21', '9999-01-01'),
(10003, 40006, '1986-08-28', '9999-01-01'),
(10004, 40054, '1986-12-01', '9999-01-01'),
(10005, 78228, '1989-09-12', '9999-01-01');

-- Insert sample data into dept_emp
INSERT INTO dept_emp (emp_no, dept_no, from_date, to_date) VALUES
(10001, 'd005', '1986-06-26', '9999-01-01'),
(10002, 'd007', '1985-11-21', '9999-01-01'),
(10003, 'd004', '1986-08-28', '9999-01-01'),
(10004, 'd004', '1986-12-01', '9999-01-01'),
(10005, 'd003', '1989-09-12', '9999-01-01');

-- Insert sample data into titles
INSERT INTO titles (emp_no, title, from_date, to_date) VALUES
(10001, 'Senior Engineer', '1986-06-26', '9999-01-01'),
(10002, 'Staff', '1985-11-21', '9999-01-01'),
(10003, 'Senior Engineer', '1986-08-28', '9999-01-01'),
(10004, 'Engineer', '1986-12-01', '9999-01-01'),
(10005, 'Senior Staff', '1989-09-12', '9999-01-01'); 