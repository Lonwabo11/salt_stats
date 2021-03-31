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
            self._connection.commit()

    def insert_science_category(self, science_category):
        """
        Insert science category of publication
        :param science_category:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """INSERT INTO ScienceCategory(ScienceCategory)
                     VALUE (%(science_category)s)
                     ON DUPLICATE KEY UPDATE ScienceCategory = %(science_category)s
                  """
            cur.execute(sql, dict(science_category=science_category))
            self._connection.commit()

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
            VALUES (%(proposal_code)s, %(principal_investigator)s, %(target)s, %(institutes)s)
            ON DUPLICATE KEY UPDATE 
            ProposalCode = %(proposal_code)s, 
            PrincipalInvestigator = %(principal_investigator)s,
            TargetOfOpportunity = %(target)s,
            Institutes = %(institutes)s
            """
            cur.execute(sql, dict(proposal_code=proposal_code,
                                  principal_investigator=principal_investigator,
                                  target=target_of_opportunity,
                                  institutes=institutes))
            self._connection.commit()

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
            self._connection.commit()

    def insert_instrument_mode(self, instrument, mode):
        """
        Insert Instrument mode used per proposal of SALT
        :param instrument: Instrument used in SALT Proposal
        :param mode: Instrument mode used in SALT Proposal
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """
            INSERT  INTO InstrumentMode(Instrument_Id, Mode) 
            VALUES ((SELECT Instrument_Id 
                    FROM Instrument
                    WHERE Instrument = %(instrument)s),
                   %(mode)s) ON DUPLICATE KEY UPDATE Mode = %(mode)s
            """
            cur.execute(sql, dict(instrument=instrument, mode=mode))
            self._connection.commit()

    def insert_time_allocating_partner(self, proposal_code, partner_name):
        """Inserting time allocating partner for a publication
        :param partner_name:
        :param proposal_code:
        :return:
        """
        with self._connection.cursor() as cur:
            sql = """
            INSERT  INTO TimeAllocatingPartner(Partner_Id, Proposal_Id) 
            VALUES ((SELECT Proposal_Id 
                    FROM Proposal
                    WHERE ProposalCode = %(proposal_code)s limit 1),
                   (SELECT Partner_Id
                    FROM Partner
                    WHERE Name = %(partner_name)s)
                    )
            """
            cur.execute(sql, dict(proposal_code=proposal_code, partner_name=partner_name))
            self._connection.commit()

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
            self._connection.commit()

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
            INSERT  INTO ScienceSubject(ScienceCategory_Id, ScienceSubject, Explanation) 
            VALUES ((SELECT ScienceCategory_Id 
                     FROM ScienceCategory
                     WHERE ScienceCategory = %(science_category)s),
                    %(science_subject)s,
                    %(explanation)s
                    )
            ON DUPLICATE KEY UPDATE ScienceSubject = %(science_subject)s, Explanation = %(explanation)s
            """
            cur.execute(sql, dict(science_subject=science_subject,
                                  explanation=explanation,
                                  science_category=science_category))
            self._connection.commit()

    def insert_proposal_instrument_use(self, publication_date, first_author_name,
                                       proposal_code, instrument_mode,
                                       year, semester, observation_date, priority,
                                       total_time, time_percentage):
        with self._connection.cursor() as cur:
            sql = """
            INSERT  INTO ProposalInstrumentUse(Publication_Id, 
                                               Proposal_Id,
                                               InstrumentMode_Id,
                                               Semester_Id,
                                               ObservationDates,
                                               Priorities,
                                               TotalSALTTime,
                                               SALTTimeFraction) 
            VALUES ((SELECT Publication_Id 
                     FROM Publication
                     WHERE PublicationDate = %(publication_date)s 
                     AND FirstAuthor_Id = (SELECT FirstAuthor_Id 
                                           FROM FirstAuthor WHERE
                                           Name = %(first_author_name)s)
                    ),
                    (SELECT Proposal_Id 
                     FROM Proposal
                     WHERE ProposalCode = %(proposal_code)s limit 1
                     ),  
                    (SELECT InstrumentMode_Id
                     FROM InstrumentMode
                     WHERE Mode = %(instrument_mode)s 
                     ),
                    (SELECT Semester_Id
                     FROM Semester
                     WHERE Year = %(year)s and Semester=%(semester)s
                     ),
                    %(observation_date)s,
                    %(priorities)s,
                    %(total_time)s,
                    %(time_percentage)s
                    )
            """
            cur.execute(sql, dict(publication_date=publication_date,
                                  first_author_name=first_author_name,
                                  proposal_code=proposal_code,
                                  instrument_mode=instrument_mode,
                                  year=year,
                                  semester=semester,
                                  observation_date=observation_date,
                                  priorities=priority,
                                  total_time=total_time,
                                  time_percentage=time_percentage
                                  ))
            self._connection.commit()

    def get_proposal_id(self, proposal_code):
        with self._connection.cursor() as cur:
            sql = """SELECT  Proposal_Id 
                     FROM Proposal
                     WHERE ProposalCode = %(proposal_code)s
                     """
            cur.execute(sql, dict(proposal_code=proposal_code))
            results = cur.fetchall()
            return results

    def insert_student_project(self, proposal_code, msc_project, phd_project):
        proposal_id = self.get_proposal_id(proposal_code)
        if proposal_id:
            with self._connection.cursor() as cur:
                sql = """
                INSERT INTO StudentProjects(Proposal_Id, MSc_Projects, PhD_projects) 
                VALUES (%(proposal_id)s,
                        %(MSc_project)s,
                        %(PhD_project)s)
                ON DUPLICATE KEY UPDATE Proposal_Id = %(proposal_id)s, 
                                                   Msc_Projects= %(MSc_project)s,
                                                   PhD_Projects = %(PhD_project)s
                 """
                cur.execute(sql, dict(proposal_id=proposal_id,
                                      MSc_project=msc_project,
                                      PhD_project=phd_project))
                self._connection.commit()

    def insert_publication_institute(self, publication_date,
                                     institute,
                                     first_author_name,
                                     first_author_belonging,
                                     other_author_belonging):
        with self._connection.cursor() as cur:
            sql = """
            INSERT INTO PublicationInstitute(Publication_Id, 
                                             Institute,
                                             FirstAuthorBelonging,
                                             OtherAuthorsBelonging)
            VALUES(
                   (SELECT Publication_Id 
                    FROM Publication 
                    WHERE PublicationDate = %(publication_date)s
                    AND FirstAuthor_Id = (SELECT FirstAuthor_Id 
                                          FROM FirstAuthor
                                          WHERE Name = %(first_author_name)s)
                    ),
                   %(institute)s,
                   %(first_author_belonging)s,
                   %(other_author_belonging)s
                   )
                  """
            cur.execute(sql, dict(publication_date=publication_date,
                                  first_author_name=first_author_name,
                                  institute=institute,
                                  first_author_belonging=first_author_belonging,
                                  other_author_belonging=other_author_belonging))
            self._connection.commit()

    def insert_first_author(self, name, position):
        with self._connection.cursor() as cur:
            sql = """
            INSERT INTO FirstAuthor(Name, Position_id)
            VALUES (%(name)s, 
                   (SELECT Position_Id 
                    FROM FirstAuthorPosition
                    WHERE AuthorPosition = %(position)s)
                   )
            ON DUPLICATE KEY UPDATE Name = %(name)s, Position_Id = (SELECT Position_Id 
                                                                    FROM FirstAuthorPosition
                                                                    WHERE AuthorPosition = %(position)s)"""
            cur.execute(sql, dict(name=name, position=position))
            self._connection.commit()

    def get_publication_type_id(self, publication_type):
        with self._connection.cursor() as cur:
            sql = """SELECT  PublicationType_Id
                     FROM PublicationType
                     WHERE PublicationType = %(publication_type)s
                 """
            cur.execute(sql, dict(publication_type=publication_type))
            results = cur.fetchall()
            return results

    def insert_publication(self, author_name, publication_date, ads_link, science_subject, publication_type,
                           authors, number_of_sa, comments):
        if self.get_publication_type_id(publication_type):
            with self._connection.cursor() as cur:
                sql = """
                INSERT INTO Publication(FirstAuthor_Id, PublicationDate, ADSLink, PublicationType_id, 
                                        ScienceSubject_Id, Authors, NumberOfSAs, Comments)
                VALUES ((SELECT FirstAuthor_Id
                         FROM FirstAuthor
                         WHERE Name = %(author_name)s
                         ),
                         %(publication_date)s,
                         %(ads_link)s,
                         (SELECT ScienceSubject_Id 
                          FROM ScienceSubject
                          WHERE ScienceSubject = %(science_subject)s
                          ),
                         (SELECT PublicationType_Id
                          FROM PublicationType
                          WHERE PublicationType = %(publication_type)s
                          ),
                         %(authors)s,
                         %(number_of_sa)s,
                         %(comments)s)
                """
                cur.execute(sql, dict(author_name=author_name,
                                      publication_date=publication_date,
                                      ads_link=ads_link,
                                      science_subject=science_subject,
                                      publication_type=publication_type,
                                      authors=authors,
                                      number_of_sa=number_of_sa,
                                      comments=comments))
                self._connection.commit()

    def insert_publication_partner(self, publication_date, first_author_name, partner_name,
                                   first_author_belonging, other_author_belonging):
        with self._connection.cursor() as cur:
            sql = """
                    INSERT INTO PublicationPartner(Publication_Id,
                                                   Partner_Id,
                                                   FirstAuthorBelonging, 
                                                   OtherAuthorsBelonging)                           
                    VALUES ((SELECT Publication_Id 
                             FROM Publication
                             WHERE PublicationDate = %(publication_date)s 
                             AND FirstAuthor_Id = (SELECT  FirstAuthor_Id
                                                   FROM FirstAuthor
                                                   WHERE Name = %(first_author_name)s)),
                             (SELECT Partner_Id 
                              FROM Partner
                              WHERE Name = %(partner_name)s
                             ),
                             %(first_author_belonging)s,
                             %(other_author_belonging)s)
                  """
            cur.execute(sql, dict(publication_date=publication_date,
                                  first_author_name=first_author_name,
                                  partner_name=partner_name,
                                  first_author_belonging=first_author_belonging,
                                  other_author_belonging=other_author_belonging))
            self._connection.commit()

    def insert_actual_issues_with_proposals(self, issue):
        with self._connection.cursor() as cur:
            sql = """ INSERT INTO IssuesForProposals(Issue)
                      VALUE (%(issue)s) ON DUPLICATE KEY UPDATE Issue = %(issue)s
                  """
            cur.execute(sql, dict(issue=issue))
            self._connection.commit()

    def insert_actual_issues_with_publications(self, issue):
        with self._connection.cursor() as cur:
            sql = """ INSERT INTO IssuesForPublications(Issue)
                      VALUE (%(issue)s) ON DUPLICATE KEY UPDATE Issue = %(issue)s 
                  """
            cur.execute(sql, dict(issue=issue))
            self._connection.commit()

    def insert_proposal_issues(self, proposal_code, issue):
        proposal_id = self.get_proposal_id(proposal_code)
        if proposal_id:
            with self._connection.cursor() as cur:
                sql = """
                INSERT INTO ProposalIssues(Proposal_Id, Issue_Id) 
                VALUES ((SELECT Proposal_Id FROM Proposal WHERE ProposalCode = %(proposal_code)s),
                        (SELECT Issue_Id FROM IssuesForProposals WHERE Issue = %(issue)s)
                        )    
                """
                cur.execute(sql, dict(proposal_code=proposal_code, issue=issue))
                self._connection.commit()

    def insert_publication_issues(self, publication_date, first_author_name, issue):
        with self._connection.cursor() as cur:
            sql = """
            INSERT INTO PublicationIssues(Publication_Id, Issue_Id)
            VALUES ((SELECT Publication_Id 
                     FROM Publication
                     WHERE PublicationDate = %(publication_date)s 
                     AND FirstAuthor_Id = (SELECT FirstAuthor_Id 
                                           FROM FirstAuthor
                                           WHERE Name = %(first_author_name)s)),
                    (SELECT Issue_Id
                     FROM IssuesForPublications
                     WHERE Issue = %(issue)s)
                    )
            """
            cur.execute(sql, dict(publication_date=publication_date,
                                  first_author_name=first_author_name,
                                  issue=issue))
            self._connection.commit()
