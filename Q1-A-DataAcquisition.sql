--Q1. Retrieve data from Stack Overflow. 


--USED TO RETRIEVE THE HIGHEST VIEWED POST. HIGHEST VIEWCOUNT WAS 8164990
SELECT TOP 50000 * 
FROM posts 
WHERE posts.ViewCount > 1000000 
ORDER BY posts.ViewCount

--RETRIEVES THE TOP 50,000 post. FROM THE RESULTS, I TOOK NOTE OF THE LOWEST VIEWCOUNT VALUE WHICH WAS 95940. I downloaded the results as a csv file called top50.csv

SELECT top 50000 * 
FROM posts as p
WHERE 
  p.ViewCount < 8164990
ORDER BY p.ViewCount DESC

--I SEARCHED FOR ONLY POSTS WITH VIEW 95940 TO ENSURE I WASN'T CUTTING OUT ANY POSTS WITH MY TOP 50000

SELECT COUNT(*)
FROM posts
WHERE 
	ViewCount = 95940 


--I THEN RAN THE FOLLOWING TO GET THE NEXT TOP 50,000 RESULTS. I DOWNLOADED THE RESULTS AS A CSV FILE CALLED TOP50TO100.CSV. TOOK NOTE OF LOWEST VIEWCOUNT WHICH WAS 56543. 

SELECT top 50000 * 
FROM posts as p
WHERE 
  p.ViewCount < 95940
ORDER BY p.ViewCount DESC

--SAME AS ABOVE. RAN TO ENSURE I WASN'T EXCLUDING ANY POSTS.

SELECT COUNT(*)
FROM posts
WHERE 
	ViewCount = 56543 


--RETRIEVE NEXT 50,000. TOOK NOTE OF LOWEST VIEWCOUNT WHICH WAS 40489. SAVED FILE AS TOP100TO150.CSV

SELECT top 50000 * 
FROM posts as p
WHERE 
  p.ViewCount < 56543
ORDER BY p.ViewCount DESC


--SAME AS ABOVE. RAN TO ENSURE I WASN'T EXCLUDING ANY POSTS.

SELECT COUNT(*)
FROM posts
WHERE 
	ViewCount = 56543 
	
--RETRIEVE NEXT 50,000. LOWEST VIEWCOUNT 31587. SAVED AS TOP150TO200.CSV
SELECT top 50000 * 
FROM posts as p
WHERE 
  p.ViewCount < 40489
ORDER BY p.ViewCount DESC

--CONFIRM NOT MISSING ANY VALUES.
SELECT COUNT(*)
FROM posts
WHERE 
	ViewCount = 31587 
	
--I created a cluster on Google Cloud Platform in Dataproc called cluster-mycluster. 
--I opened the VM Instance of the Master node. 
--I uploaded each csv as seen in the screenshot.
