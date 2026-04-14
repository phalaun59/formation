DECLARE
    v_test_count NUMBER := 0;
    v_schema_name VARCHAR2(100);
BEGIN
    -- Récupération du nom du schéma actuel
    SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO v_schema_name FROM DUAL;
    
    -- Une petite boucle bidon
    FOR i IN 1..5 LOOP
        v_test_count := v_test_count + i;
    END LOOP;

    -- On affiche un log dans la console de sortie Oracle
    -- (Si tu as activé le DBMS_OUTPUT)
    DBMS_OUTPUT.PUT_LINE('Test réussi sur le schéma : ' || v_schema_name);
    
    -- Un COMMIT pour tester la validation
    COMMIT;
END;