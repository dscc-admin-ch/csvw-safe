# (Notes from) Guidelines from Open Data Support (European Commission)

[Designing and developing RDF vocabularies](https://data.europa.eu/sites/default/files/d2.1.2_training_module_2.4_designing_and_developing_vocabularies_in_rdf_en_edp.pdf)

RDF vocabulary: A vocabulary is a data model comprising classes, properties and relationships which can be used for describing your data and metadata.

RDF Vocabularies are sets of terms used to describe things.
A term is either a class or a property
- Object type properties (relationships)
- Data type properties (attributes) -- in our case, only attributes (To verify)

Properties begin with a lower case letter, e.g. rdfs:label.
Data type properties should be nouns, e.g. dcterms:description.
Use camel case if a term has more than one word, e.g. foaf:isPrimaryTopicOf

### Steps for modelling data
1. Start with a robust Domain Model developed following a structured process and methodology.
2. Research existing terms and their usage and maximise reuse of those terms. Reusable RDF vocabularies on [Linked Open Vocabulary](https://lov.linkeddata.es/dataset/lov/).
4. Where new terms can be seen as specialisations of existing terms, create sub class and sub properties.
5. Where new terms are required, create them following commonly agreed best practice.
6. Publish within a highly stable environment designed to be persistent. Choose a stable namespace for your RDF schema (e.g. W3C, Purl...). Use good practices on the publication of persistent Uniform  Resource Identifiers (URI) sets, both in terms of format and of their design rules and management.
7. Publicise the RDF schema by registering it with relevant services (Joinup and Linked Open Vocabularies).

See [this](https://interoperable-europe.ec.europa.eu/collection/semic-support-centre/document/process-and-methodology-developing-core-vocabularies).


### Already existing vocabulary:
['Privacy' in search engine](https://lov.linkeddata.es/dataset/lov/terms?q=privacy)

**DCAT**: Describing Dataset: recommends DCAT but here we mean something else. We want to describe the 'inside' of the dataset not the way to share it. 

**dcterms:accessRights**: access or restrictions based on privacy, security, or other policies

**dpv:DifferentialPrivacy**: https://w3c.github.io/dpv/2.2/dpv/#DifferentialPrivacy. But more legal, consent, residual risk, access control management. Also has a 'Data & Personal Data' part but more about is sensitive, confidential or other data.

Other vocabulary have fields about privacy but coarse and table level.


## Validation and adding constraints
[SHACL](https://openmetadatastandards.org/rdf/shapes/overview/) (Shapes Constraint Language) validates RDF data.

This [document on W3C](https://www.w3.org/TR/shacl/) defines the SHACL Shapes Constraint Language, a language for validating RDF graphs against a set of conditions.

RDF metadata validation rules define constraints (like data types, formats, cardinality, and relationships) for RDF graphs, primarily using W3C standards like SHACL (Shapes Constraint Language) for expressing shapes and rules, with older methods like SPIN being superseded; these rules ensure data quality, consistency, and interoperability, often checking against specific vocabularies like DCAT-AP or schema.org.
