SELECT city_name, ROUND(AVG(salary), 1) AS average_salary
FROM (employee AS Emp
LEFT OUTER JOIN city as C
ON Emp.city_id = C.city_id)
WHERE seniority = 'Junior'
GROUP BY city_name
ORDER BY average_salary DESC;

SELECT position_name, ROUND(AVG(salary), 1) AS average_salary
FROM (employee AS Emp
LEFT OUTER JOIN city AS C ON Emp.city_id = C.city_id
LEFT OUTER JOIN "position" AS P ON Emp.position_id = P.position_id)
GROUP BY position_name
ORDER BY average_salary DESC;

SELECT city_name, ROUND(AVG(salary), 1) AS average_salary
FROM (employee AS Emp
LEFT OUTER JOIN city as C
ON Emp.city_id = C.city_id)
WHERE gender = 'Female'
GROUP BY city_name
ORDER BY average_salary DESC;

SELECT city_name, 
ROUND(AVG(salary), 1) AS average_salary, 
ROUND(AVG(vacation_days), 1) AS avg_vacation_days
FROM (employee AS E
LEFT OUTER JOIN city AS C ON E.city_id = C.city_id)
GROUP BY city_name
ORDER BY avg_vacation_days DESC;

SELECT 
C.city_name,
    Co.company_size,
    ROUND(AVG(E.salary), 1) AS average_salary
FROM (employee AS E
JOIN city AS C ON E.city_id = C.city_id
JOIN company AS Co ON E.company_id = Co.company_id)
GROUP BY C.city_name, Co.company_size
ORDER BY average_salary DESC;



