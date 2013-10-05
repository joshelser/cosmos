tree grammar CosmosResultSet;

options {
	language=Java;
	tokenVocab=CosmosSQL;
	ASTLabelType=CommonTree;
}

cosmos_specific returns [ResultSetNode e]
   : supported_statements{ e = $supported_statements.e;}
;

show_tables:
	SHOW TABLES
;


supported_statements returns [ResultSetNode e]
   : show_tables  { e = new ShowTableExp(); }
;
