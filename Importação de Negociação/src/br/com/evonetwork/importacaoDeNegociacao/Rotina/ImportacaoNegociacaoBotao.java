package br.com.evonetwork.importacaoDeNegociacao.Rotina;

import java.io.File;
import java.math.BigDecimal;
import java.sql.ResultSet;

import com.sankhya.util.JdbcUtils;
import com.sankhya.util.TimeUtils;

import br.com.evonetwork.importacaoDeNegociacao.Controller.ControllerImportacao;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class ImportacaoNegociacaoBotao implements AcaoRotinaJava {
	
	//Botão na AD_IMPNEG

	@Override
	public void doAction(ContextoAcao ca) throws Exception {
		System.out.println("***EVO - IMPORTAÇÃO DE NEGOCIAÇÃO - INICIO***");
		if(ca.getLinhas().length > 1)
			throw new Exception("Selecione uma linha por vez!");
		Registro linha = ca.getLinhas()[0];
		iniciarImportacaoNegociacao(ca, linha);
		System.out.println("***EVO - IMPORTAÇÃO DE NEGOCIAÇÃO - FIM***");
	}

	private void iniciarImportacaoNegociacao(ContextoAcao ca, Registro linha) throws Exception {
        BigDecimal nroUnicoImportacao = (BigDecimal) linha.getCampo("NROUNICO");
        String importado = (String) linha.getCampo("IMPORTADO");
        System.out.println("Iniciando importacao da Planilha - Nro único: "+nroUnicoImportacao);
        if (importado != null && importado.equals("S"))
            throw new Exception("Ação não permitida.\n\nO arquivo já foi importado!");
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
			
			sql.appendSql("SELECT TOP 1 CHAVEARQUIVO, NOMEARQUIVO,\r\n"
					+ "CASE WHEN UPPER(NOMEARQUIVO) LIKE '%XLSX%' THEN 'S' ELSE 'N' END FORMATO_XLSX,\r\n"
					+ "(SELECT TEXTO FROM TSIPAR WHERE CHAVE = 'CAMINHOARQIMP') CAMINHO\r\n"
					+ "FROM TSIANX\r\n"
					+ "WHERE NOMEINSTANCIA = 'AD_IMPNEG'\r\n"
					+ "AND PKREGISTRO LIKE '"+nroUnicoImportacao+"_%'");
			System.out.println("SQL: "+sql.toString());
			
			rset = sql.executeQuery();
			
			if (rset.next()) {
				String caminho = rset.getString("CAMINHO");
				String arquivo = rset.getString("CHAVEARQUIVO");
				String nomeArquivo = rset.getString("NOMEARQUIVO");
	            String formatoXlsx = rset.getString("FORMATO_XLSX");
	            System.out.println("Caminho: "+caminho);
	            System.out.println("Arquivo: "+arquivo);
	            System.out.println("Nome arquivo: "+nomeArquivo);
	            System.out.println("Formato XLSX? "+formatoXlsx);
	            File file = new File(caminho+arquivo);
	            System.out.println("Arquivo Existe: "+file.exists());
	            if (formatoXlsx.equals("S")) {
	            	String msgRetorno = ControllerImportacao.importarDadosPlanilha(file, ca, nroUnicoImportacao);
	                finalizarImportacao(nroUnicoImportacao, msgRetorno, nomeArquivo, ca);
	            } else {
	            	throw new Exception("Falha na importação da planilha: Apenas planilhas no formato XLSX são permitidas!");
	            }
	        } else {
	        	throw new Exception("O arquivo para importação não foi localizado!");
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
	}

	private void finalizarImportacao(BigDecimal nroUnico, String msgRetorno, String nomeArquivo, ContextoAcao ca) throws Exception {
		if (msgRetorno == null || msgRetorno.isEmpty())
            ca.setMensagemRetorno("Planilha importada com sucesso");
        else
            ca.setMensagemRetorno("Planilha importada com erros, verifique o log de importação.");
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			JapeFactory.dao("AD_IMPNEG").prepareToUpdateByPK(nroUnico)
				.set("MSGLOG", msgRetorno)
				.set("CODUSU", ca.getUsuarioLogado())
				.set("DHALTER", TimeUtils.getNow())
				.set("NOMEARQUIVO", nomeArquivo)
				.set("IMPORTADO", "S")
				.update();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JapeSession.close(hnd);
		}
	}

}
