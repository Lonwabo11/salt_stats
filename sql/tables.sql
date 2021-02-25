
DROP TABLE IF EXISTS FirstAuthorPosition;

CREATE  TABLE  FirstAuthorPosition(
        Position_Id  INT AUTO_INCREMENT PRIMARY KEY,
        Position VARCHAR(40)
);

CREATE UNIQUE INDEX idx_position ON FirstAuthorPosition(Position);

DROP TABLE IF EXISTS FirstAuthor;

CREATE TABLE  FirstAuthor(
        FirstAuthor_Id  INT PRIMARY KEY AUTO_INCREMENT,
        Name  VARCHAR(40),
        Position_Id INT,
        FOREIGN KEY (Position_Id) REFERENCES FirstAuthorPosition(Position_Id)
);

CREATE UNIQUE INDEX idx_name ON FirstAuthor(Name);

DROP TABLE IF EXISTS PublicationType;

CREATE TABLE PublicationType(
        PublicationType_Id INT PRIMARY KEY AUTO_INCREMENT,
        PublicationType VARCHAR(40)
);

CREATE UNIQUE INDEX idx_publicationType ON PublicationType(PublicationType);

DROP TABLE IF EXISTS ScienceCategory;

CREATE TABLE ScienceCategory(
        ScienceCategory_Id INT PRIMARY KEY AUTO_INCREMENT,
        ScienceCategory VARCHAR(40)
);
CREATE UNIQUE INDEX idx_scienceCategory ON ScienceCategory(ScienceCategory);

DROP TABLE IF EXISTS ScienceSubject;

CREATE TABLE  ScienceSubject(
        ScienceSubject_Id INT PRIMARY KEY AUTO_INCREMENT,
        ScienceCategory_Id INT,
        ScienceSubject VARCHAR(30),
        Explanation VARCHAR(30),
        FOREIGN KEY (ScienceCategory_Id)REFERENCES ScienceCategory(ScienceCategory_Id)
);

DROP TABLE IF EXISTS Publication;

CREATE TABLE  Publication(
        Publication_Id INT AUTO_INCREMENT PRIMARY KEY,
        FirstAuthor_Id INT,
        PublicationDate date,
        ADSLink VARCHAR(255),
        PublicationType_Id INT,
        ScienceSubject_Id INT,
        Authors text,
        NumberOfSAs INT,
        Comments VARCHAR(255),
        FOREIGN KEY (FirstAuthor_Id) REFERENCES FirstAuthor(FirstAuthor_Id),
        FOREIGN KEY (PublicationType_Id) REFERENCES PublicationType(PublicationType_Id),
        FOREIGN KEY (ScienceSubject_Id) REFERENCES ScienceSubject(ScienceSubject_Id)
);


DROP TABLE IF EXISTS Partner;

CREATE TABLE  Partner(
        Partner_Id INT PRIMARY KEY AUTO_INCREMENT,
        Name VARCHAR(40)
);
CREATE UNIQUE INDEX idx_name ON Partner(Name);


DROP TABLE IF EXISTS PublicationPartner;

CREATE TABLE  PublicationPartner(
        Publication_Id INT,
        Partner_Id INT,
        FirstAuthorBelonging BOOLEAN,
        OtherAuthorsBelonging BOOLEAN,
        FOREIGN KEY (Publication_Id) REFERENCES Publication(Publication_Id),
        FOREIGN KEY (Partner_Id) REFERENCES Partner(Partner_Id)
);

DROP TABLE IF EXISTS PublicationInstitute;

CREATE TABLE  PublicationInstitute(
        Publication_Id INT,
        Institute VARCHAR(40),
        FirstAuthorBelonging BOOLEAN,
        OtherAuthorsBelonging BOOLEAN
);


DROP TABLE IF EXISTS Proposal;

CREATE TABLE  Proposal(
        Proposal_Id INT PRIMARY KEY AUTO_INCREMENT,
        ProposalCode VARCHAR(40),
        PrincipalInvestigator VARCHAR(40),
        TargetOfOpportunity BOOLEAN,
        Institutes VARCHAR(40)
);

DROP TABLE IF EXISTS StudentProjects;

CREATE TABLE  StudentProjects(
        Proposal_Id INT,
        MSc_Projects INT,
        PhD_Projects INT,
        FOREIGN KEY (Proposal_Id) REFERENCES Proposal(Proposal_Id)
);

DROP TABLE IF EXISTS TimeAllocatingPartner;

CREATE TABLE  TimeAllocatingPartner(
        Partner_Id INT,
        Proposal_Id INT,
        FOREIGN KEY (Partner_Id) REFERENCES Partner(Partner_Id),
        FOREIGN KEY (Proposal_Id) REFERENCES Proposal(Proposal_Id)
);

CREATE TABLE  Instrument(
        Instrument_Id INT PRIMARY KEY AUTO_INCREMENT,
        Instrument VARCHAR(40)
);

CREATE UNIQUE INDEX idx_instrument ON Instrument(Instrument);

DROP TABLE IF EXISTS InstrumentMode;

CREATE TABLE  InstrumentMode(
        InstrumentMode_Id INT PRIMARY KEY AUTO_INCREMENT,
        Instrument_Id INT,
        Mode VARCHAR(30),
        FOREIGN KEY (Instrument_Id) REFERENCES Instrument(Instrument_Id)
);
CREATE UNIQUE INDEX idx_instrumentMode ON InstrumentMode (Mode);


DROP TABLE IF EXISTS Semester;

CREATE TABLE  Semester(
        Semester_Id INT AUTO_INCREMENT PRIMARY KEY,
       Year INT,
       Semester INT
);

CREATE UNIQUE INDEX idx_semester ON Semester (Year, Semester);

DROP TABLE IF EXISTS ProposalInstrumentUse;

CREATE TABLE  ProposalInstrumentUse(
      Publication_Id INT,
      Proposal_Id INT,
      InstrumentMode_Id INT,
      Semester_Id INT,
      ObservationDates DATE,
      Priorities INT,
      TotalSALTTime INT,
      SALTTimeFraction FLOAT,
      FOREIGN KEY (Proposal_Id) REFERENCES Proposal(Proposal_Id),
      FOREIGN KEY (Publication_Id) REFERENCES Publication(Publication_Id),
      FOREIGN KEY (InstrumentMode_Id)REFERENCES InstrumentMode(InstrumentMode_Id),
      FOREIGN KEY (Semester_Id) REFERENCES Semester(Semester_Id)
);

CREATE TABLE  ProposalIssues(
       Proposal_Id INT,
       Issue_Id INT,
       FOREIGN KEY (Proposal_Id) REFERENCES Proposal(Proposal_Id)
);

CREATE TABLE  PublicationIssues(
        Publication_Id INT,
        Issue_Id INT,
        FOREIGN KEY (Publication_Id) REFERENCES Publication(Publication_Id)
);


CREATE TABLE ProposalIssues(
        Proposal_Id INT,
        Issue_Id INT,
        FOREIGN KEY (Proposal_Id) REFERENCES Proposal(Proposal_Id)
);

CREATE TABLE PublicationIssues(
        Publication_Id INT,
        Issue_Id INT,
        FOREIGN KEY (Publication_Id) REFERENCES Publication(Publication_Id)
);
