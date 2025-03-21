-- Create transformation log table
CREATE OR REPLACE PROCEDURE create_transformation_log IS
    -- Local variables for error handling
    v_error_message VARCHAR2(4000);
    v_error_code NUMBER;
BEGIN
    -- Drop existing objects if they exist
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE transformation_log';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -942 THEN RAISE; END IF;
    END;

    BEGIN
        EXECUTE IMMEDIATE 'DROP SEQUENCE transformation_log_seq';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE != -2289 THEN RAISE; END IF;
    END;

    -- Create the transformation log table
    EXECUTE IMMEDIATE 'CREATE TABLE transformation_log (
        log_id NUMBER,
        transformation_name VARCHAR2(100),
        operation_type VARCHAR2(50),
        field_name VARCHAR2(100),
        original_value VARCHAR2(4000),
        transformed_value VARCHAR2(4000),
        record_id NUMBER,
        record_type VARCHAR2(50),
        status VARCHAR2(20) DEFAULT ''SUCCESS'',
        error_message VARCHAR2(4000),
        created_date TIMESTAMP DEFAULT SYSTIMESTAMP,
        created_by VARCHAR2(100) DEFAULT USER
    )';

    -- Create sequence for log_id
    EXECUTE IMMEDIATE 'CREATE SEQUENCE transformation_log_seq
        START WITH 1
        INCREMENT BY 1
        NOCACHE
        NOCYCLE';

    -- Create trigger for log_id
    EXECUTE IMMEDIATE 'CREATE OR REPLACE TRIGGER trg_transformation_log_bir
        BEFORE INSERT ON transformation_log
        FOR EACH ROW
    BEGIN
        IF :new.log_id IS NULL THEN
            :new.log_id := transformation_log_seq.NEXTVAL;
        END IF;
    END;';

    -- Create index for common queries
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_trans_name ON transformation_log(transformation_name)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_record_id ON transformation_log(record_id)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_status ON transformation_log(status)';
    EXECUTE IMMEDIATE 'CREATE INDEX idx_transformation_log_created_date ON transformation_log(created_date)';

    -- Add comments
    EXECUTE IMMEDIATE 'COMMENT ON TABLE transformation_log IS ''Logs all data transformations with detailed tracking''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.log_id IS ''Unique identifier for the log entry''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.transformation_name IS ''Name of the transformation being performed''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.operation_type IS ''Type of operation (INSERT, UPDATE, DELETE, TRANSFORM)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.field_name IS ''Name of the field being transformed''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.original_value IS ''Original value before transformation''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.transformed_value IS ''Value after transformation''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.record_id IS ''ID of the record being transformed''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.record_type IS ''Type of record being transformed''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.status IS ''Status of the transformation (SUCCESS, ERROR, WARNING)''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.error_message IS ''Error message if transformation failed''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.created_date IS ''Timestamp of the log entry''';
    EXECUTE IMMEDIATE 'COMMENT ON COLUMN transformation_log.created_by IS ''User who performed the transformation''';

    -- Create procedure to log transformations
    EXECUTE IMMEDIATE 'CREATE OR REPLACE PROCEDURE log_transformation(
        p_transformation_name IN VARCHAR2,
        p_operation_type IN VARCHAR2,
        p_field_name IN VARCHAR2,
        p_original_value IN VARCHAR2,
        p_transformed_value IN VARCHAR2,
        p_record_id IN NUMBER,
        p_record_type IN VARCHAR2,
        p_status IN VARCHAR2 DEFAULT ''SUCCESS'',
        p_error_message IN VARCHAR2 DEFAULT NULL
    ) IS
    BEGIN
        INSERT INTO transformation_log (
            transformation_name,
            operation_type,
            field_name,
            original_value,
            transformed_value,
            record_id,
            record_type,
            status,
            error_message
        ) VALUES (
            p_transformation_name,
            p_operation_type,
            p_field_name,
            p_original_value,
            p_transformed_value,
            p_record_id,
            p_record_type,
            p_status,
            p_error_message
        );
    END;';

EXCEPTION
    WHEN OTHERS THEN
        v_error_message := SQLERRM;
        v_error_code := SQLCODE;
        RAISE_APPLICATION_ERROR(-20001, 'Error creating transformation log: ' || v_error_message);
END create_transformation_log;
/

-- Add comment
COMMENT ON PROCEDURE create_transformation_log IS 'Creates the transformation logging structure with proper error handling'; 