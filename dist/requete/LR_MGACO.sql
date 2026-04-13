SELECT
    TRIM(CLI_CDMAG) || ';' ||
    TRIM(ACO_AAAC) || ';' ||
    TRIM(ACO_CDAC) || ';' ||
    TRIM(ACO_TYORI) || ';' ||
    TRIM(ACO_LBACCOM) || ';' ||
    TRIM(ACO_DTDEBUT )  || ';' ||
    TRIM(ACO_DTFIN  )  || ';' ||
    '$'
FROM MGACO,MGMAG WHERE aco_aaac=26 AND aco_cdac in ('11','12', '011', '012') and ACO_TYORI = 'N';