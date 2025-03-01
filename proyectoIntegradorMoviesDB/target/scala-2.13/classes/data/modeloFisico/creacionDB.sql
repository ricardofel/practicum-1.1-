DROP SCHEMA IF EXISTS movie_db; -- ELIMINAR SI EXISTE UNA DB CON EL MISMO NOMBRE

-- CREAR EL ESQUEMA
CREATE SCHEMA movie_db
  DEFAULT CHARACTER SET utf8
  DEFAULT COLLATE utf8_general_ci; 

-- USAR EL ESQUEMA CREADO
USE movie_db; 

-- CREAR LAS TABLAS EN EL ORDEN RESPECTIVO PARA RESPETAR LAS RESTRICCIONES DE CLAVES
CREATE TABLE collection (
    id_collection INT PRIMARY KEY COMMENT 'Identificador único de la colección',
    name VARCHAR(255) NOT NULL COMMENT 'Nombre de la colección',
    poster_path VARCHAR(255) COMMENT 'Ruta del póster de la colección',
    backdrop_path VARCHAR(255) COMMENT 'Ruta del fondo asociado a la colección'
);

CREATE TABLE movie (
    id_movie INT PRIMARY KEY COMMENT 'Identificador único de la película',
    title VARCHAR(255) COMMENT 'Título oficial de la película',
    original_title VARCHAR(255) COMMENT 'Título original de la película',
    imdb_id VARCHAR(50) COMMENT 'Identificador único de la película en IMDb',
    budget BIGINT COMMENT 'Presupuesto de la película en dólares',
    revenue DECIMAL(18,2) COMMENT 'Ingresos totales de la película en dólares',
    original_language CHAR(2) COMMENT 'Idioma original de la película (código ISO_639_1)',
    overview TEXT COMMENT 'Resumen breve de la trama de la película',
    release_date DATE COMMENT 'Fecha de estreno de la película (YYYY-MM-DD)',
    run_time INT COMMENT 'Duración de la película en minutos',
    status VARCHAR(50) COMMENT 'Estado de la película (por ejemplo, Released)',
    tagline TEXT NULL COMMENT 'Frase promocional de la película',
    video BOOLEAN COMMENT 'Indica si hay un video asociado en TMDB',
    adult BOOLEAN COMMENT 'Indica si la película tiene contenido para adultos',
    popularity DECIMAL(10,6) COMMENT 'Puntaje de popularidad asignado por TMDB',
    poster_path VARCHAR(255) NULL COMMENT 'Ruta del póster de la película',
    homepage VARCHAR(255) NULL COMMENT 'Página oficial de la película',
    vote_average DECIMAL(3,1) COMMENT 'Calificación promedio basada en votos (TMDB)',
    vote_count INT COMMENT 'Número total de votos recibidos (TMDB)',
    id_collection INT NULL COMMENT 'ID de la colección a la que pertenece la película',
    FOREIGN KEY (id_collection) REFERENCES collection(id_collection)
);

CREATE TABLE rating (
    rating_id INT PRIMARY KEY COMMENT 'Identificador único de la calificación',        
    userId INT NOT NULL COMMENT 'ID del usuario que realizó la calificación',             
    rating DECIMAL(2,1) NOT NULL COMMENT 'Calificación otorgada (escala de 1-5) (Movie_Lens)',      
    timestamp BIGINT NOT NULL COMMENT 'Momento en que se realizó la calificación',       
    id_movie INT NOT NULL COMMENT 'ID de la película calificada',           
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie) 
);

CREATE TABLE language (
    id_language CHAR(2) PRIMARY KEY COMMENT 'Código ISO_639_1 del idioma', 
    name VARCHAR(100) NOT NULL COMMENT 'Nombre del idioma (por ejemplo, English)'           
);

CREATE TABLE language_movie (
    id_language CHAR(2) COMMENT 'Código ISO_639_1 del idioma asociado a la película',
    id_movie INT COMMENT 'ID de la película',
    PRIMARY KEY (id_language, id_movie),
    FOREIGN KEY (id_language) REFERENCES language(id_language),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie)
);

CREATE TABLE keyword (
    id_keyword INT PRIMARY KEY COMMENT 'Identificador único de la palabra clave',
    name VARCHAR(100) NOT NULL COMMENT 'Nombre de la palabra clave'
);

CREATE TABLE keyword_movie (
    id_keyword INT COMMENT 'ID de la palabra clave asociada a la película',
    id_movie INT COMMENT 'ID de la película',
    PRIMARY KEY (id_keyword, id_movie),
    FOREIGN KEY (id_keyword) REFERENCES keyword(id_keyword),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie)
);

CREATE TABLE gender (
    id_gender INT PRIMARY KEY COMMENT 'Identificador único del género',
    name VARCHAR(100) NOT NULL COMMENT 'Nombre del género (por ejemplo, Comedy)'
);

CREATE TABLE gender_movie (
    id_gender INT COMMENT 'ID del género asociado a la película',
    id_movie INT COMMENT 'ID de la película',
    PRIMARY KEY (id_gender, id_movie),
    FOREIGN KEY (id_gender) REFERENCES gender(id_gender),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie)
);

CREATE TABLE country (
    id_country CHAR(2) PRIMARY KEY COMMENT 'Código ISO_3166_1 del país de producción',
    name VARCHAR(100) NOT NULL COMMENT 'Nombre del país de producción'
);

CREATE TABLE country_movie (
    id_country CHAR(2) COMMENT 'Código ISO_3166_1 del país asociado a la película',
    id_movie INT COMMENT 'ID de la película',
    PRIMARY KEY (id_country, id_movie),
    FOREIGN KEY (id_country) REFERENCES country(id_country),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie)
);

CREATE TABLE company (
    id_company INT PRIMARY KEY COMMENT 'Identificador único de la compañía de producción',
    name VARCHAR(255) NOT NULL COMMENT 'Nombre de la compañía de producción'
);

CREATE TABLE company_movie (
    id_company INT COMMENT 'ID de la compañía de producción asociada',
    id_movie INT COMMENT 'ID de la película',
    PRIMARY KEY (id_company, id_movie),
    FOREIGN KEY (id_company) REFERENCES company(id_company),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie)
);

CREATE TABLE cast_miembros (
    id_person BIGINT PRIMARY KEY COMMENT 'ID único de la persona en TMDB',
    name VARCHAR(255) NOT NULL COMMENT 'Nombre del actor',
    gender INT COMMENT 'Género del actor (0: no especificado, 1: femenino, 2: masculino)',
    profile_path VARCHAR(255) COMMENT 'Ruta de la imagen del perfil del actor'
);

CREATE TABLE cast_movie (
    credit_id CHAR(30) COMMENT 'Identificador único del crédito de la persona en una pelicula en TMDB',
    cast_id SMALLINT COMMENT 'Identificador del actor dentro de la película',
    character_movie VARCHAR(255) COMMENT 'Personaje interpretado por el actor',
    order_movie SMALLINT COMMENT 'Orden de aparición del actor en la película',
    id_person BIGINT COMMENT 'ID único de la persona en TMDB',
    id_movie INT COMMENT 'ID de la película',
    PRIMARY KEY (id_person, credit_id),
    FOREIGN KEY (id_person) REFERENCES cast_miembros(id_person),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie)
);

CREATE TABLE crew_miembros (
    id_person BIGINT PRIMARY KEY COMMENT 'ID único de la persona en TMDB',
    name VARCHAR(255) NOT NULL COMMENT 'Nombre del miembro del equipo',
    gender INT COMMENT 'Género del miembro del equipo (0: no especificado, 1: femenino, 2: masculino)',
    profile_path VARCHAR(255) COMMENT 'Ruta de la imagen del perfil del miembro del equipo'
);

CREATE TABLE job_department (
    id_job INT PRIMARY KEY COMMENT 'ID único del trabajo en el equipo de producción',
    job VARCHAR(100) NOT NULL COMMENT 'Nombre del trabajo (por ejemplo, Director)',
    department VARCHAR(100) NOT NULL COMMENT 'Nombre del departamento (por ejemplo, Directing)'
);

CREATE TABLE crew_movie (
    id_person BIGINT COMMENT 'ID único de la persona en TMDB',
    id_movie INT COMMENT 'ID de la película',
    credit_id CHAR(30) COMMENT 'Identificador único del crédito de la persona en una pelicula en TMDB',
    id_job INT COMMENT 'ID del trabajo realizado por el miembro del equipo',
    PRIMARY KEY (id_person, credit_id),
    FOREIGN KEY (id_person) REFERENCES crew_miembros(id_person),
    FOREIGN KEY (id_movie) REFERENCES movie(id_movie),
    FOREIGN KEY (id_job) REFERENCES job_department(id_job)
);