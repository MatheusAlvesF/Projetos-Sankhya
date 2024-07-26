package br.com.evonetwork.importacaoDeNegociacao.Utils;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.util.ArrayList;

import com.sankhya.util.JdbcUtils;

import br.com.evonetwork.importacaoDeNegociacao.Model.CamposConfiguracao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class BuscarDados {

	public static int buscarLinhaInicial(String codConfig) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		int linhaInicial = 0;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT ISNULL(LINHAINICIAL,1) - 1 AS LINHAINICIAL FROM AD_IMPNEGCAB WHERE NROUNICO = "+codConfig);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next()) {
				BigDecimal linhaInicialBD = rset.getBigDecimal("LINHAINICIAL");
				System.out.println("Linha inicial query: "+linhaInicialBD);
				if(linhaInicialBD != null && linhaInicialBD.compareTo(BigDecimal.ZERO) > 0)
					linhaInicial = linhaInicialBD.intValue();
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			rset.getStatement().close();
			rset.close();
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return linhaInicial;
	}

	public static ArrayList<CamposConfiguracao> buscarCamposConfiguracao(String codConfig) throws Exception {
		ArrayList<CamposConfiguracao> camposConfiguracao = new ArrayList<CamposConfiguracao>();
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT (COLUNA - 1) AS COLUNA, TABELA, CAMPO, TIPO, QTDDECIMAIS FROM AD_IMPNEGITE WHERE NROUNICO = "+codConfig+" ORDER BY COLUNA");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			while (rset.next()) {
				CamposConfiguracao campoConfiguracao =  new CamposConfiguracao();
				campoConfiguracao.setColuna(rset.getBigDecimal("COLUNA"));
				campoConfiguracao.setTabela(rset.getString("TABELA"));
				campoConfiguracao.setNome(rset.getString("CAMPO"));
				campoConfiguracao.setTipo(rset.getString("TIPO"));
				campoConfiguracao.setQtdDecimais(rset.getBigDecimal("QTDDECIMAIS"));
				camposConfiguracao.add(campoConfiguracao);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			rset.getStatement().close();
			rset.close();
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return camposConfiguracao;
	}

	public static String buscarNomeInstanciaDaTabela(String tabela) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		String dado = "";
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT NOMEINSTANCIA FROM TDDINS WHERE NOMETAB = '"+tabela+"'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				dado = rset.getString("NOMEINSTANCIA");
			else
				throw new Exception("Não foi encontrada nenhuma tabela com o nome: "+tabela);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return dado;
	}

	public static boolean buscarSeProspectExiste(String cgc_cpf) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT 1 FROM TCSPAP WHERE CGC_CPF = '"+cgc_cpf+"'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				return true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			rset.getStatement().close();
			rset.close();
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return false;
	}

	public static BigDecimal buscarCodigoBairro(String bairro) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codBairro = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODBAI FROM TSIBAI WHERE NOMEBAI LIKE '%"+bairro.toUpperCase()+"%'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codBairro = rset.getBigDecimal("CODBAI");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codBairro;
	}

	public static boolean buscarSeProdutoPertenceASolucao(BigDecimal codProd, BigDecimal codSolucao) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT 1 FROM AD_TCSMETSOL WHERE CODSOL = "+codSolucao+" AND CODPROD = "+codProd);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				return true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			rset.getStatement().close();
			rset.close();
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return false;
	}

	public static BigDecimal buscarCodigoProspect(String cgc_cpf) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codPap = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODPAP FROM TCSPAP WHERE CGC_CPF = '"+cgc_cpf+"'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codPap = rset.getBigDecimal("CODPAP");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			rset.getStatement().close();
			rset.close();
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codPap;
	}

	public static boolean buscarSeExisteEsteVendedorNoProspect(BigDecimal codPap, BigDecimal codSolucao) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT * FROM AD_VENDTIPOSOLUCAOPROSP WHERE CODPAP = "+codPap+" AND CODSOL = "+codSolucao);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next()) {
				System.out.println("Já existe um vendedor para este tipo de solução...");
				return true;
			} else {
				System.out.println("Não existe vendedor para este tipo de solução...");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return false;
	}

	public static BigDecimal buscarNegociacaoAbertaParaSolucao(BigDecimal codPap, BigDecimal codSolucao) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal numOS = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT NUMOS FROM TCSOSE WHERE CODPAP = "+codPap+" AND CODMETOD IN (SELECT CODMETOD FROM AD_TCSMETSOL WHERE CODSOL = "+codSolucao+") AND SITUACAO <> 'F'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next()) {
				numOS = rset.getBigDecimal("NUMOS");
				System.out.println("Existe uma negociação aberta para este tipo de solução: "+numOS+"...");
			} else {
				System.out.println("Não existe uma negociação aberta para este tipo de solução...");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return numOS;
	}

	public static BigDecimal buscarUsuarioDoVendedor(BigDecimal codVend) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codUsu = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT MAX(CODUSU) AS CODUSU FROM TSIUSU WHERE CODVEND = "+codVend);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codUsu = rset.getBigDecimal("CODUSU");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codUsu;
	}

	public static int buscarNegociacaoRelacionadaInicio(String cgc_cpf, BigDecimal nroUnicoImportacao) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal negRelacInic = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT COUNT(*) AS QTD\r\n"
					+ "FROM TCSPAP P, TCSOSE OSE, TCSITE ITE, TCSMOD A\r\n"
					+ "WHERE P.CODPAP = OSE.CODPAP\r\n"
					+ "AND OSE.CODPAP = P.CODPAP\r\n"
					+ "AND OSE.SITUACAO = 'P'\r\n"
					+ "AND OSE.NUMOS = ITE.NUMOS\r\n"
					+ "AND ITE.NUMITEM = (SELECT MAX(NUMITEM) FROM TCSITE WHERE NUMOS = OSE.NUMOS)\r\n"
					+ "AND A.CODMETOD = (SELECT INTEIRO FROM TSIPAR WHERE CHAVE = 'SERVMETPRE')\r\n"
					+ "AND A.CODPROD = ITE.CODSERV\r\n"
					+ "AND A.NUMSEQ = 1\r\n"
					+ "AND P.CGC_CPF LIKE SUBSTRING('"+cgc_cpf+"',1,8)+'%'\r\n"
					+ "AND P.CGC_CPF <> '"+cgc_cpf+"'\r\n"
					+ "AND ISNULL(P.AD_NROUNICO,0) <>  "+nroUnicoImportacao+"\r\n"
					+ "AND ISNULL(P.AD_NROUNICOVINC,0) <>  "+nroUnicoImportacao+"");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				negRelacInic = rset.getBigDecimal("QTD");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return negRelacInic.intValue();
	}

	public static int buscarNegociacaoRelacionadaOutros(String cgc_cpf, BigDecimal nroUnicoImportacao) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal negRelacOutr = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT COUNT(*) AS QTD\r\n"
					+ "FROM TCSPAP P, TCSOSE OSE, TCSITE ITE, TCSMOD A\r\n"
					+ "WHERE P.CODPAP = OSE.CODPAP\r\n"
					+ "AND OSE.CODPAP = P.CODPAP\r\n"
					+ "AND OSE.SITUACAO = 'P'\r\n"
					+ "AND OSE.NUMOS = ITE.NUMOS\r\n"
					+ "AND ITE.NUMITEM = (SELECT MAX(NUMITEM) FROM TCSITE WHERE NUMOS = OSE.NUMOS)\r\n"
					+ "AND A.CODMETOD = (SELECT INTEIRO FROM TSIPAR WHERE CHAVE = 'SERVMETPRE')\r\n"
					+ "AND A.CODPROD = ITE.CODSERV\r\n"
					+ "AND A.NUMSEQ > 1\r\n"
					+ "AND P.CGC_CPF LIKE SUBSTRING('"+cgc_cpf+"',1,8)+'%'\r\n"
					+ "AND P.CGC_CPF <> '"+cgc_cpf+"'\r\n"
					+ "AND ISNULL(P.AD_NROUNICO,0) <>  "+nroUnicoImportacao+"\r\n"
					+ "AND ISNULL(P.AD_NROUNICOVINC,0) <>  "+nroUnicoImportacao+"");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				negRelacOutr = rset.getBigDecimal("QTD");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return negRelacOutr.intValue();
	}

	public static BigDecimal buscarProspectMatriz(String cgc_cpf, BigDecimal nroUnicoImportacao) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codPap = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT MAX(CODPAP) AS CODPAP FROM TCSPAP WHERE CGC_CPF = (\r\n"
					+ "SELECT MIN(P.CGC_CPF)\r\n"
					+ "FROM TCSPAP P\r\n"
					+ "JOIN TCSOSE OSE ON P.CODPAP = OSE.CODPAP\r\n"
					+ "WHERE P.CGC_CPF LIKE SUBSTRING('"+cgc_cpf+"',1,8)+'%'\r\n"
					+ "AND OSE.SITUACAO = 'P'\r\n"
					+ "AND P.CGC_CPF <> '"+cgc_cpf+"'\r\n"
					+ "AND ISNULL(P.AD_NROUNICO,0) <>  "+nroUnicoImportacao+"\r\n"
					+ "AND ISNULL(P.AD_NROUNICOVINC,0) <>  "+nroUnicoImportacao+"\r\n"
					+ "AND P.AD_CODPAPMATRIZ IS NULL)");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codPap = rset.getBigDecimal("CODPAP");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codPap;
	}

	public static BigDecimal buscarCodigoSituacao(BigDecimal codProd) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codSit = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODSIT FROM TCSSES WHERE AD_STATUSINCLUSAO = 'S' AND CODPROD = "+codProd);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codSit = rset.getBigDecimal("CODSIT");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codSit;
	}

	public static BigDecimal buscarEtapaDoProdutoPelaSequencia(BigDecimal codProd, int sequencia) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codServ = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODPROD FROM TCSMOD WHERE CODMETOD = (SELECT CODMETOD FROM AD_TCSMETSOL WHERE CODPROD = "+codProd+") AND NUMSEQ = "+sequencia);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codServ = rset.getBigDecimal("CODPROD");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codServ;
	}

	public static BigDecimal buscarMotivoDoServico(BigDecimal codServ) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codOcorOS = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODOCOROS FROM TCSSEM WHERE CODPROD = "+codServ);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codOcorOS = rset.getBigDecimal("CODOCOROS");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codOcorOS;
	}

	public static BigDecimal buscarSolucaoDaConfiguracao(String codConfig) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codSol = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODSOL FROM AD_IMPNEGCAB WHERE NROUNICO = "+codConfig);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codSol = rset.getBigDecimal("CODSOL");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codSol;
	}

	public static boolean buscarSeVendedorPertenceAoCanal(BigDecimal codVend, BigDecimal canal) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT 1 FROM AD_VENDEDORCANAL WHERE ATIVO = 'S' AND CODVEND = "+codVend+" AND IDCANAL = "+canal);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next()) {
				System.out.println("Existe vínculo entre o vendedor "+codVend+" e canal "+canal+"...");
				return true;
			} else {
				System.out.println("Não existe vínculo entre o vendedor "+codVend+" e canal "+canal+"...");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return false;
	}

	public static BigDecimal buscarEndereco(String endereco, String codTipoEndereco) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codEnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODEND FROM TSIEND WHERE NOMEEND LIKE '"+endereco+"' AND TIPO = '"+codTipoEndereco+"'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codEnd = rset.getBigDecimal("CODEND");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codEnd;
	}

	public static String buscarCodigoTipoEndereco(String tipoEndereco) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		String codTipoEndereco = "";
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT TIPO FROM TSITEND WHERE (DESCRICAO LIKE '"+tipoEndereco+"') OR (TIPO LIKE '"+tipoEndereco+"')");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codTipoEndereco = rset.getString("TIPO");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codTipoEndereco;
	}

	public static BigDecimal buscarCidade(String cidade, BigDecimal codUF) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codCid = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT MAX(CODCID) AS CODCID FROM TSICID WHERE NOMECID LIKE '"+cidade+"' AND UF = "+codUF);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codCid = rset.getBigDecimal("CODCID");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codCid;
	}

	public static BigDecimal buscarCodigoModeloDoProduto(BigDecimal codProd) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal codMetod = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT CODMETOD FROM AD_TCSMETSOL WHERE CODPROD = "+codProd);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				codMetod = rset.getBigDecimal("CODMETOD");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return codMetod;
	}

	public static String buscarCampoTextoDoProspectExistente(String campo, BigDecimal codPap) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		String valor = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT "+campo+" FROM TCSPAP WHERE CODPAP = "+codPap);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				valor = rset.getString(campo);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return valor;
	}

	public static BigDecimal buscarCampoInteiroDoProspectExistente(String campo, BigDecimal codPap) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		BigDecimal valor = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT "+campo+" FROM TCSPAP WHERE CODPAP = "+codPap);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				valor = rset.getBigDecimal(campo);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return valor;
	}

	public static boolean buscarSeOrigemEhElegivelParaAtualizar(BigDecimal codOat) throws Exception {
		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		ResultSet rset = null;
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();
			
			sql = new NativeSql(jdbc);
			
			sql.appendSql("SELECT 1 FROM AD_ORIGENSELEGIVEIS WHERE CODOAT = "+codOat);
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next())
				return true;
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JdbcUtils.closeResultSet(rset);
			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);
		}
		return false;
	}

}
