create database labs_inidividual_01;
use labs_inidividual_01;
SELECT idSucursal , avg(precio) as promedio
FROM precios
group by idSucursal
having idSucursal = '688';
