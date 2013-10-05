// $ANTLR 3.5.1 CosmosSQL.g 2013-10-04 21:59:59

package cosmos.sql.parser;
import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.tree.*;


@SuppressWarnings("all")
public class CosmosSQLParser extends Parser {
	public static final String[] tokenNames = new String[] {
		"<invalid>", "<EOR>", "<DOWN>", "<UP>", "ALL_FIELDS", "AND_SYM", "ARROW", 
		"ASTERISK", "A_", "BITAND", "BIT_NUM", "B_", "COLON", "COMMA", "C_", "DIVIDE", 
		"DOT", "D_", "EQ_SYM", "E_", "F_", "GET", "GTH", "G_", "HEX_DIGIT", "HEX_DIGIT_FRAGMENT", 
		"H_", "ID", "INTEGER_NUM", "I_", "J_", "K_", "LBRACK", "LET", "LPAREN", 
		"LTH", "L_", "MINUS", "MOD_SYM", "M_", "NEGATION", "NOT_EQ", "N_", "OR_SYM", 
		"O_", "PLUS", "POWER_OP", "P_", "Q_", "RBRACK", "REAL_NUMBER", "RPAREN", 
		"R_", "SEMI", "SET_VAR", "SHIFT_LEFT", "SHIFT_RIGHT", "SHOW", "S_", "Space", 
		"TABLES", "TEXT_STRING", "T_", "U_", "VERTBAR", "V_", "W_", "X_", "Y_", 
		"Z_"
	};
	public static final int EOF=-1;
	public static final int ALL_FIELDS=4;
	public static final int AND_SYM=5;
	public static final int ARROW=6;
	public static final int ASTERISK=7;
	public static final int A_=8;
	public static final int BITAND=9;
	public static final int BIT_NUM=10;
	public static final int B_=11;
	public static final int COLON=12;
	public static final int COMMA=13;
	public static final int C_=14;
	public static final int DIVIDE=15;
	public static final int DOT=16;
	public static final int D_=17;
	public static final int EQ_SYM=18;
	public static final int E_=19;
	public static final int F_=20;
	public static final int GET=21;
	public static final int GTH=22;
	public static final int G_=23;
	public static final int HEX_DIGIT=24;
	public static final int HEX_DIGIT_FRAGMENT=25;
	public static final int H_=26;
	public static final int ID=27;
	public static final int INTEGER_NUM=28;
	public static final int I_=29;
	public static final int J_=30;
	public static final int K_=31;
	public static final int LBRACK=32;
	public static final int LET=33;
	public static final int LPAREN=34;
	public static final int LTH=35;
	public static final int L_=36;
	public static final int MINUS=37;
	public static final int MOD_SYM=38;
	public static final int M_=39;
	public static final int NEGATION=40;
	public static final int NOT_EQ=41;
	public static final int N_=42;
	public static final int OR_SYM=43;
	public static final int O_=44;
	public static final int PLUS=45;
	public static final int POWER_OP=46;
	public static final int P_=47;
	public static final int Q_=48;
	public static final int RBRACK=49;
	public static final int REAL_NUMBER=50;
	public static final int RPAREN=51;
	public static final int R_=52;
	public static final int SEMI=53;
	public static final int SET_VAR=54;
	public static final int SHIFT_LEFT=55;
	public static final int SHIFT_RIGHT=56;
	public static final int SHOW=57;
	public static final int S_=58;
	public static final int Space=59;
	public static final int TABLES=60;
	public static final int TEXT_STRING=61;
	public static final int T_=62;
	public static final int U_=63;
	public static final int VERTBAR=64;
	public static final int V_=65;
	public static final int W_=66;
	public static final int X_=67;
	public static final int Y_=68;
	public static final int Z_=69;

	// delegates
	public Parser[] getDelegates() {
		return new Parser[] {};
	}

	// delegators


	public CosmosSQLParser(TokenStream input) {
		this(input, new RecognizerSharedState());
	}
	public CosmosSQLParser(TokenStream input, RecognizerSharedState state) {
		super(input, state);
	}

	protected TreeAdaptor adaptor = new CommonTreeAdaptor();

	public void setTreeAdaptor(TreeAdaptor adaptor) {
		this.adaptor = adaptor;
	}
	public TreeAdaptor getTreeAdaptor() {
		return adaptor;
	}
	@Override public String[] getTokenNames() { return CosmosSQLParser.tokenNames; }
	@Override public String getGrammarFileName() { return "CosmosSQL.g"; }


		@Override
		protected Object recoverFromMismatchedToken(IntStream intput, int ttype,BitSet follow) throws RecognitionException{
			throw new MismatchedTokenException(ttype, input);
		}

		@Override
		public Object recoverFromMismatchedSet(IntStream intput, RecognitionException e, BitSet follow) throws RecognitionException{
			throw e;
		}


	public static class show_tables_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "show_tables"
	// CosmosSQL.g:154:1: show_tables : SHOW TABLES ;
	public final CosmosSQLParser.show_tables_return show_tables() throws RecognitionException {
		CosmosSQLParser.show_tables_return retval = new CosmosSQLParser.show_tables_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		Token SHOW1=null;
		Token TABLES2=null;

		Object SHOW1_tree=null;
		Object TABLES2_tree=null;

		try {
			// CosmosSQL.g:154:12: ( SHOW TABLES )
			// CosmosSQL.g:155:2: SHOW TABLES
			{
			root_0 = (Object)adaptor.nil();


			SHOW1=(Token)match(input,SHOW,FOLLOW_SHOW_in_show_tables1148); 
			SHOW1_tree = (Object)adaptor.create(SHOW1);
			adaptor.addChild(root_0, SHOW1_tree);

			TABLES2=(Token)match(input,TABLES,FOLLOW_TABLES_in_show_tables1150); 
			TABLES2_tree = (Object)adaptor.create(TABLES2);
			adaptor.addChild(root_0, TABLES2_tree);

			}

			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

			catch(RecognitionException e){
			throw e;
			}

		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "show_tables"


	public static class supported_statements_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "supported_statements"
	// CosmosSQL.g:158:1: supported_statements : show_tables ;
	public final CosmosSQLParser.supported_statements_return supported_statements() throws RecognitionException {
		CosmosSQLParser.supported_statements_return retval = new CosmosSQLParser.supported_statements_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		ParserRuleReturnScope show_tables3 =null;


		try {
			// CosmosSQL.g:158:21: ( show_tables )
			// CosmosSQL.g:159:2: show_tables
			{
			root_0 = (Object)adaptor.nil();


			pushFollow(FOLLOW_show_tables_in_supported_statements1159);
			show_tables3=show_tables();
			state._fsp--;

			adaptor.addChild(root_0, show_tables3.getTree());

			}

			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

			catch(RecognitionException e){
			throw e;
			}

		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "supported_statements"


	public static class cosmos_specific_return extends ParserRuleReturnScope {
		Object tree;
		@Override
		public Object getTree() { return tree; }
	};


	// $ANTLR start "cosmos_specific"
	// CosmosSQL.g:162:1: cosmos_specific : supported_statements ;
	public final CosmosSQLParser.cosmos_specific_return cosmos_specific() throws RecognitionException {
		CosmosSQLParser.cosmos_specific_return retval = new CosmosSQLParser.cosmos_specific_return();
		retval.start = input.LT(1);

		Object root_0 = null;

		ParserRuleReturnScope supported_statements4 =null;


		try {
			// CosmosSQL.g:162:16: ( supported_statements )
			// CosmosSQL.g:163:2: supported_statements
			{
			root_0 = (Object)adaptor.nil();


			pushFollow(FOLLOW_supported_statements_in_cosmos_specific1168);
			supported_statements4=supported_statements();
			state._fsp--;

			adaptor.addChild(root_0, supported_statements4.getTree());

			}

			retval.stop = input.LT(-1);

			retval.tree = (Object)adaptor.rulePostProcessing(root_0);
			adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

		}

			catch(RecognitionException e){
			throw e;
			}

		finally {
			// do for sure before leaving
		}
		return retval;
	}
	// $ANTLR end "cosmos_specific"

	// Delegated rules



	public static final BitSet FOLLOW_SHOW_in_show_tables1148 = new BitSet(new long[]{0x1000000000000000L});
	public static final BitSet FOLLOW_TABLES_in_show_tables1150 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_show_tables_in_supported_statements1159 = new BitSet(new long[]{0x0000000000000002L});
	public static final BitSet FOLLOW_supported_statements_in_cosmos_specific1168 = new BitSet(new long[]{0x0000000000000002L});
}
