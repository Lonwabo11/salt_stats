from database_configuration import DatabaseConfiguration
from MySQLdb import connect


class DatabaseInsertion:

    def __init__(self, database_config: DatabaseConfiguration):
        self._connection = connect(
            user=database_config.username(),
            password=database_config.password(),
            host=database_config.host(),
            port=database_config.port(),
            database=database_config.database()
        )
        self._cursor = self._connection.cursor()

    def insert_partner(self, partner_name):
        """
        Insert name of partner for first author
        :param partner_name:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """INSERT INTO Partner(Name) 
                     VALUE(%(partner_name)s)
                     ON DUPLICATE KEY UPDATE Name=%(partner_name)s"""
            cur.execute(sql, dict(partner_name=partner_name))

    def insert_position(self, position):
        """
        Insert position of First Author for a publication
        :param position:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """INSERT INTO FirstAuthorPosition(position)
                     VALUE %(position)s
                     ON DUPLICATE KEY UPDATE Position = %(position)s
                     """
            cur.execute(sql, dict(position=position))

    def insert_science_category(self, science_category):
        """
        Insert science category of publication
        :param science_category:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """INSERT INTO ScienceCategory(ScienceCategory)
                     VALUE %(science_category)s
                     ON DUPLICATE KEY UPDATE ScienceCategory = %(science_category)s
                  """
            cur.execute(sql, dict(science_category=science_category))

    def insert_publication_type(self, publication_type):
        """
        Insert type of publication for a SALT paper
        :param publication_type:
        :return:
        """

    def insert_proposal(self, proposal_code, principal_investigator, target_of_opportunity, institutes):
        """
        Insert proposal information which makes the SALT publication(s)
        :param proposal_code:
        :param principal_investigator:
        :param target_of_opportunity:
        :param institutes:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """
            INSERT  INTO Proposal(ProposalCode, PrincipalInvestigator,TargetOfOpportunity,Institutes) 
            VALUES (%(proposal_code)s, %(prinicipal_investigator)s, %(target)s, %(institutes)s)
            """
            cur.execute(sql, dict(proposal_code=proposal_code,
                                  principal_investigator=principal_investigator,
                                  target=target_of_opportunity,
                                  institutes=institutes))

    def insert_semester(self, year, semester):
        """
        Insert the semester for a SALT Proposal
        :param year: Year of the SALT proposal
        :param semester: Semester which SALT proposal was in
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """ INSERT INTO Semester(Year, Semester) 
                      VALUES (%(year)s, %(semester)s)
                      ON DUPLICATE KEY UPDATE Year=%(year)s,Semester=%(semester)s"""
            cur.execute(sql, dict(year=year, semester=semester))

    def insert_instrument_mode(self, instrument, mode):
        """
        Insert Instrument mode used per proposal of SALT
        :param instrument: Instrument used in SALT Proposal
        :param mode: Instrument mode used in SALT Proposal
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """
            WITH 
            instr (Instrument_Id) AS (
                 SELECT Instrument_Id 
                 FROM Instrument
                 WHERE Instrument = %(instrument)s
            )
            INSERT  INTO InstrumentMode(Instrument_Id, Mode) 
            VALUES ((SELECT Instrument_Id FROM instr), %(instrument_mode)s) ON DUPLICATE KEY UPDATE Mode = %(mode)s
            """
            cur.execute(sql, dict(instrument=instrument, instrument_mode=mode))

    def insert_time_allocating_partner(self, proposal_code, partner_name):
        """Inserting time allocating partner for a publication
        :param partner_name:
        :param proposal_code:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """
            WITH prop_id (Proposal_Id) AS (
                 SELECT Proposal_Id 
                 FROM Proposal
                 WHERE ProposalCode = %(proposal_code)s
            ),
            pid (Partner_Id) AS ( 
             SELECT Partner_Id
             FROM Partner
             WHERE Name = %(partner_name)s
             )
            INSERT  INTO TimeAllocatingPartner(Partner_Id, Proposal_Id) 
            VALUES ((SELECT Proposal_Id FROM prop_id),
                    (SELECT Partner_Id FROM pid))
            """
            cur.execute(sql, dict(proposal_code=proposal_code, partner_name=partner_name))

    def insert_instrument(self, instrument):
        """
        Insert instrument used in proposal for publication
        :param instrument:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """ INSERT INTO Instrument(Instrument) 
                      VALUE (%(instrument)s)
                      ON DUPLICATE KEY UPDATE Instrument=%(instrument)s"""
            cur.execute(sql, dict(instrument=instrument))

    def insert_science_subject(self, science_subject, explanation, science_category):
        """
        Insert science subject of SALT publication
        :param science_category:
        :param science_subject:
        :param explanation:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """
            WITH category (ScienceCategory_Id) AS (
                 SELECT ScienceCategory_Id 
                 FROM ScienceSubject
                 WHERE ScienceSubject = %(science_subject)s
            )
            INSERT  INTO ScienceSubject(ScienceCategory_Id, ScienceSubject, Explanation) 
            VALUES ((SELECT ScienceCategory_Id FROM category),
                    %(science_subject)s,
                    %(explanation)s)
            """
            cur.execute(sql, dict(science_subject=science_subject,
                                  explanation=explanation,
                                  science_category=science_category))

    def insert_proposal_instrument_use(self, publication, proposal_code, instrument_mode,
                                       year, semester, observation_date, priority,
                                       total_time, time_percentage):
        with self._connection.cursor() as cur:
            sql = """
            WITH
             pulication (Publication_Id) AS (
                 SELECT Publication_Id 
                 FROM Publication
                 WHERE PublicationDate = %(publication)s
            ),
            prop_id (Proposal_Id) AS (
                 SELECT Proposal_Id 
                 FROM Proposal
                 WHERE ProposalCode = %(proposal_code)s
            ),
            instr_mode (InstrumentMode_Id) AS (
                      SELECT InstrumentMode_Id
                      FROM InstrumentMode
                      WHERE Mode = %(instrument_mode)s 
            ),
            semester(Semester_Id) AS (
                    SELECT Semester_Id
                    FROM Semester
                    WHERE Year = %(year)s and Semester=%(semester)s 
            )
            INSERT  INTO ProposalInstrumentUse(Publication_Id, 
                                               Proposal_Id,
                                               InstrumentMode_Id,
                                               Semester_Id,
                                               ObservationDates,
                                               Priorities,
                                               TotalSALTTime,
                                               SALTTimeFraction) 
            VALUES ((SELECT Publication_Id FROM publication),
                    (SELECT Proposal_Id FROM prop_id),  
                    (SELECT InstrumentMode_Id FROM instr_mode),
                    (SELECT Semester_Id FROM semester),
                    %(observation_date)s,
                    %(priorities)s,
                    %(total_time)s,
                    %(time_percentage)s)
            """
            cur.execute(sql, dict(publication=publication,
                                  proposal_code=proposal_code,
                                  instrument_mode=instrument_mode,
                                  year=year,
                                  semester=semester,
                                  observation_date=observation_date,
                                  priorities=priority,
                                  total_time=total_time,
                                  time_percentage=time_percentage
                                  ))

    def insert_student_project(self, proposal_code, msc_project, phd_project):
        with self._connection.cursor() as cur:
            sql = """
            WITH 
            prop_id (Proposal_Id) AS (
                 SELECT  Proposal_Id 
                 FROM Proposal
                 WHERE ProposalCode = %(proposal_code)s
            ) 
            INSERT INTO StudentProjects(Proposal_Id, MSc_Projects, PhD_projects) 
            VALUES ((SELECT Proposal_Id FROM prop_id),
                     %(MSc_project)s,
                     %(PhD_project)s)"""
            cur.execute(sql, dict(proposal_code=proposal_code,
                                  MSc_project=msc_project,
                                  PhD_project=phd_project))

    def insert_publication_institute(self, publication, institute, first_author, other_author):
        with self._connection.cursor() as cur:
            sql = """
            WITH
             pub_id (Publication_Id) AS (
                 SELECT Publication_Id 
                 FROM Publication 
                 WHERE PublicationDate = %(publication)s          
            )              
            INSERT INTO PublicationInstitute(Publication_Id, 
                                             Institute,
                                             FirstAuthorBelonging,
                                             OtherAuthorsBelonging)
            VALUES(
                   (SELECT Publication_Id  FROM pub_id),
                   %(institute)s,
                   %(first_author)s,
                   %(other_author)s
                   )
                  """
            cur.execute(sql, dict(publication=publication,
                                  institute=institute,
                                  first_author=first_author,
                                  other_author=other_author))

    def insert_first_author(self, name, position):
        with self._connection.cursor() as cur:
            sql = """
            WITH 
            pos_id (Position_Id) AS (
                 SELECT Position_Id 
                 FROM FirstAuthorPosition
                 WHERE Position = %(position)s
            )
            INSERT INTO FirstAuthor(Name, Position_id)
            VALUES (%(name)s, 
                   (SELECT Position_Id from pos_id))
            """
            cur.execute(sql, dict(name=name, position=position))

    def insert_publication(self, author_name, science_subject, publication_type, publication_date,
                           ads_link, authors, number_of_sa, comments):
        with self._connection.cursor() as cur:
            sql = """
            WITH
             author_id (FirstAuthor_Id) AS (
                 SELECT FirstAuthor_Id
                 FROM FirstAuthor
                 WHERE Name = %(author_name)s
            ),
            science_subject (ScienceSubject_Id) AS (
                  SELECT ScienceSubject_Id 
                  FROM ScienceSubject
                  WHERE ScienceSubject = %(science_subject)s         
            ),
            pub_type (PublicationType_Id) AS (
                    SELECT PublicationType_Id
                    FROM PublicationType
                    WHERE PublicationType = %(publication_type)s
            )
            INSERT INTO Publication(FirstAuthor_Id, PublicationDate, ADSLink, PublicationType_id, 
                                    ScienceSubject_Id, Authors, NumberOfSAs, Comments)
            VALUES ((SELECT FirstAuthor_Id FROM author_id),
                     %(publication_date)s,
                     %(ads_link)s,
                     (SELECT PublicationType_Id FROM pub_type),
                     (SELECT ScienceSubject_Id FROM science_subject),
                     %(authors)s,
                     %(number_of_sa)s,
                     %(comments)s)
            """
            cur.execute(sql, dict(author_name=author_name,
                                  science_subject=science_subject,
                                  publication_type=publication_type,
                                  publication_date=publication_date,
                                  authors=authors,
                                  ads_link=ads_link,
                                  number_of_sa=number_of_sa,
                                  comments=comments))

    def insert_publication_partner(self, publication, partner_name, first_author, other_author):
        with self._connection.cursor() as cur:
            sql = """
                    WITH 
                    pub_id (Publication_Id) AS (
                         SELECT Publication_Id 
                         FROM Publication
                         WHERE PublicationDate = %(publication)s
                    ),
                    partner (Partner_Id) AS (
                            SELECT Partner_Id 
                            FROM Partner
                            WHERE Name = %(partner_name)s
                    )  
                    INSERT INTO PublicationPartner(Publication_Id,Partner_Id, FirstAuthorBelonging, 
                                                    OtherAuthorsBelonging)                           
                    VALUES ((SELECT Publication_Id FROM pub_id),
                             (SELECT Partner_Id FROM partner),
                             %(first_author)s,
                             %(other_author)s)
                  """
            cur.execute(sql, dict(publication=publication,
                                  partner_name=partner_name,
                                  first_author=first_author,
                                  other_author=other_author))
