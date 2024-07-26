package br.com.evonetwork.importacaoDeNegociacao.Controller;

import java.io.File;
import java.io.FileInputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.sankhya.util.StringUtils;

import br.com.evonetwork.importacaoDeNegociacao.Model.CamposConfiguracao;
import br.com.evonetwork.importacaoDeNegociacao.Model.CamposImportacao;
import br.com.evonetwork.importacaoDeNegociacao.Utils.BuscarDados;
import br.com.evonetwork.importacaoDeNegociacao.Utils.ImportacaoUtils;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;

public class ControllerImportacao {

	public static String importarDadosPlanilha(File file, ContextoAcao ca, BigDecimal nroUnicoImportacao) throws Exception {
    	System.out.println("Iniciando importação da planilha...");
        String codConfig = ca.getParam("CODMODELOIMP").toString();
        boolean validarCanalDoVendedor = ("S".equals((String) ca.getParam("VALIDARCANALVENDEDOR"))) ? true : false;
        System.out.println("Validar canal do vendedor: "+validarCanalDoVendedor);
        BigDecimal codSolucao = BuscarDados.buscarSolucaoDaConfiguracao(codConfig);
        if(codSolucao == null)
        	throw new Exception("Nenhuma solução vinculada com a configuração informada!");
        StringBuilder msgRetorno = new StringBuilder().append("");
        int linhaInicial = BuscarDados.buscarLinhaInicial(codConfig);
        ArrayList<CamposConfiguracao> camposConfiguracao = BuscarDados.buscarCamposConfiguracao(codConfig);
        XSSFWorkbook workbook;
        try {
            workbook = new XSSFWorkbook(new FileInputStream(file));
        } catch (Exception e) {
            System.out.println(file);
            e.printStackTrace();
            throw new Exception("Arquivo não localizado!");
        }
        XSSFSheet sheet = workbook.getSheetAt(0);
        Iterator<Row> rowIterator = (Iterator<Row>)sheet.iterator();
        while (rowIterator.hasNext()) {
        	ArrayList<CamposImportacao> camposImportacao = new ArrayList<CamposImportacao>();
        	Row linha = rowIterator.next();
            int linhaAtual = linha.getRowNum();
            System.out.println("Linha atual: "+linhaAtual);
            System.out.println("Linha inicial: "+linhaInicial);
            if (linhaAtual >= linhaInicial) {
            	String cpf_cnpj_formatado = "";
            	int ultimaCelula = linha.getLastCellNum();
            	for (CamposConfiguracao campoConfiguracao : camposConfiguracao) {
            		CamposImportacao campoImportacao = new CamposImportacao();
            		int coluna = campoConfiguracao.getColuna().intValue();
            		Cell cell = linha.getCell(coluna);
                    boolean colunaVazia = cell == null || cell.getCellType() == 3 || (cell.getCellType() == 1 && cell.getStringCellValue().trim().isEmpty());
                    if (!colunaVazia) {
                    	if (ultimaCelula < coluna)
                            throw new Exception("Erro na configuração, a coluna "+coluna+" não existe na planilha! O modelo da importação pode ser baixado através do botão 'Modelo Planilha Imp. Unificada'.");
                    	String conteudo = null;
                    	System.out.println("Cell type: "+cell.getCellType());
                        switch (cell.getCellType()) {
                            case 0:
                                conteudo = String.valueOf(BigDecimal.valueOf(cell.getNumericCellValue()).toBigInteger());
                                break;
                            case 1:
                                conteudo = cell.getStringCellValue();
                                conteudo = conteudo.replace("'", "");
                                break;
                            case 2:
                                conteudo = cell.getStringCellValue();
                                break;
                            default:
                            	cell.setCellType(1);
                                conteudo = cell.getStringCellValue();
                                break;
                        }
                        String tabela = campoConfiguracao.getTabela();
		                String campo = campoConfiguracao.getNome();
		                String tipo = campoConfiguracao.getTipo();
		                System.out.println("Tabela: "+tabela);
		                System.out.println("Campo: "+campo);
		                System.out.println("Tipo: "+tipo);
		                System.out.println("Conteúdo: "+conteudo);
		                campoImportacao.setNomeTabela(tabela);
                    	campoImportacao.setNomeCampo(campo);
                    	campoImportacao.setTipoConteudo(tipo);
                        campoImportacao.setConteudoString(conteudo);
                        camposImportacao.add(campoImportacao);
                        if("CGC_CPF".equals(campo))
                        	cpf_cnpj_formatado = StringUtils.formataCgcCpf(conteudo);
                    } else if (campoConfiguracao.getNome().equals("CGC_CPF")) {
	        			System.out.println("Célula CGC_CPF vazia!");
	        			cpf_cnpj_formatado = "0";
	        		} else {
	        			System.out.println("Célula vazia.");
	        		}
				}
            	if (cpf_cnpj_formatado.equals("0"))
                    continue;
                try {
                	importarDadosSankhya(ca, camposImportacao, nroUnicoImportacao, codSolucao, msgRetorno, cpf_cnpj_formatado, validarCanalDoVendedor);
                } catch (Exception e) {
                    System.out.println("Erro: "+e.getMessage());
                    if (e.getMessage().equals("Preencha o campo UF para prosseguir."))
                        msgRetorno.append("<b>CPF/CNPJ "+cpf_cnpj_formatado+":</b> Informações de endereço incompletas (UF). "+"<br/>");
                    else
                        msgRetorno.append("<b>CPF/CNPJ "+cpf_cnpj_formatado+":</b> "+e.getMessage()+"<br/>");
                }
            }
        }
        System.out.println("Finalizando importação da planilha...");
        return msgRetorno.toString();
    }

	private static void importarDadosSankhya(ContextoAcao ca, ArrayList<CamposImportacao> camposImportacao, BigDecimal nroUnicoImportacao, BigDecimal codSolucao, StringBuilder msgRetorno, String cpf_cnpj_formatado, boolean validarCanalDoVendedor) throws Exception {
		System.out.println("Iniciando importação dos dados...");
		String cgc_cpf = null;
		BigDecimal codVend = null;
		BigDecimal codProd = null;
		BigDecimal codOrigem = null;
		String nomeLead = null;
		String cidade = null;
		String telefone = null;
		BigDecimal codUF = null;
		BigDecimal canal = null;
		ArrayList<CamposImportacao> camposProspect = new ArrayList<CamposImportacao>();;
		ArrayList<CamposImportacao> camposContatoProspect = new ArrayList<CamposImportacao>();;
		ArrayList<CamposImportacao> camposNegociacao = new ArrayList<CamposImportacao>();;
		for (CamposImportacao campoImportacao : camposImportacao) {
			String campo = campoImportacao.getNomeCampo();
			if("TCSPAP".equals(campoImportacao.getNomeTabela())) {
				if("CGC_CPF".equals(campo)) {
					cgc_cpf = campoImportacao.getConteudoString().replace("-", "").replace(".", "").replace("/", "").trim();
					camposProspect.add(new CamposImportacao("CGC_CPF", "TCSPAP", cgc_cpf, "A"));
				} else if (campo.equals("ENDERECO")) {
					String logradouro = campoImportacao.getConteudoString();
					if(logradouro.contains(",")) {
						String[] partes = logradouro.replaceAll(".-","").split(",");
						if(partes.length == 3) { //se tiver 3 pedaços = tem RUA, NUMERO, BAIRRO
							String rua = partes[0].trim();
							String numero = partes[1].trim();
							String bairro = partes[2].trim();
							System.out.println("Endereço separado em: Rua - "+rua.trim()+" Numero - "+numero.trim()+" Bairro - "+bairro.trim());
							camposProspect.add(new CamposImportacao("ENDERECO", "TCSPAP", rua, "A"));
							camposProspect.add(new CamposImportacao("AD_CODEND", "TCSPAP", String.valueOf(buscarOuCriarEndereco(rua).intValue()), "N"));
							System.out.println("Conteúdo Rua: "+rua);
							camposProspect.add(new CamposImportacao("NUMEND", "TCSPAP", numero, "A"));
							System.out.println("Conteúdo Núm.: "+numero);
							BigDecimal codBai = buscarOuCriarBairro(bairro);
							camposProspect.add(new CamposImportacao("AD_CODBAI", "TCSPAP", String.valueOf(codBai.intValue()), "N"));
							System.out.println("Conteúdo Bairro: "+codBai);
						} else if(partes.length == 2) {
							String rua = partes[0].trim();
							String numero = partes[1].trim();
							System.out.println("Endereço separado em: Rua - "+rua.trim()+" Numero - "+numero.trim());
							camposProspect.add(new CamposImportacao("ENDERECO", "TCSPAP", rua, "A"));
							camposProspect.add(new CamposImportacao("AD_CODEND", "TCSPAP", String.valueOf(buscarOuCriarEndereco(rua).intValue()), "N"));
							System.out.println("Conteúdo Rua: "+rua);
							camposProspect.add(new CamposImportacao("NUMEND", "TCSPAP", numero, "A"));
							System.out.println("Conteúdo Núm.: "+numero);
						} else {
							String[] partesLogradouro = logradouro.split(" ");
					        String nomeEnd = null;
					        String numEnd = partesLogradouro[partesLogradouro.length-1].trim();
					        if (numEnd.matches("[0-9]+") || numEnd.equals("S/N")){
					            nomeEnd = logradouro.replaceAll(numEnd,"").trim();
					            camposProspect.add(new CamposImportacao("ENDERECO", "TCSPAP", nomeEnd, "A"));
					            camposProspect.add(new CamposImportacao("AD_CODEND", "TCSPAP", String.valueOf(buscarOuCriarEndereco(nomeEnd).intValue()), "N"));
					            System.out.println("Conteúdo Rua: "+nomeEnd.trim());
					            camposProspect.add(new CamposImportacao("NUMEND", "TCSPAP", numEnd, "A"));
					            System.out.println("Conteúdo Núm.: "+numEnd.trim());
					        } else {
					        	String enderecoFormatado = logradouro.replaceAll(".-","").trim();
					        	camposProspect.add(new CamposImportacao("ENDERECO", "TCSPAP", enderecoFormatado, "A"));
					        	camposProspect.add(new CamposImportacao("AD_CODEND", "TCSPAP", String.valueOf(buscarOuCriarEndereco(enderecoFormatado).intValue()), "N"));
					        	System.out.println("Conteúdo: "+enderecoFormatado);
					        }
						}
					} else {
						String enderecoFormatado = logradouro.replaceAll(".-","").trim();;
			        	camposProspect.add(new CamposImportacao("ENDERECO", "TCSPAP", enderecoFormatado, "A"));
			        	camposProspect.add(new CamposImportacao("AD_CODEND", "TCSPAP", String.valueOf(buscarOuCriarEndereco(enderecoFormatado).intValue()), "N"));
			        	System.out.println("Conteúdo: "+enderecoFormatado);
			        }
				} else if (campo.equals("CODUF")) {
					String UF = campoImportacao.getConteudoString().replaceAll(".-","").trim();
					SessionHandle hnd = null;
					try {
						hnd = JapeSession.open();
						JapeWrapper ufDAO = JapeFactory.dao(DynamicEntityNames.UNIDADE_FEDERATIVA);
						Collection<DynamicVO> dynamicVOs = ufDAO.find("UF = ?", UF);
						DynamicVO ufs = (DynamicVO) (dynamicVOs.isEmpty() ? null : dynamicVOs.iterator().next());
						codUF = (BigDecimal) ufs.getProperty("CODUF");
						camposProspect.add(new CamposImportacao("CODUF", "TCSPAP", String.valueOf(codUF.intValue()), "N"));
						System.out.println("Conteúdo: "+codUF);
					} finally {
						JapeSession.close(hnd);
					}
				} else if (campo.equals("CODVEND")) {
					codVend = new BigDecimal(campoImportacao.getConteudoString());
				} else if (campo.equals("TELEFONE")) {
					telefone = campoImportacao.getConteudoString().replace("(", "").replace(")", "").replace("-", "").replace(" ", "").trim();
					camposProspect.add(new CamposImportacao("TELEFONE", "TCSPAP", telefone, "A"));
				} else if (campo.equals("NOMEPAP")) {
					nomeLead = campoImportacao.getConteudoString().trim();
					camposProspect.add(new CamposImportacao("NOMEPAP", "TCSPAP", nomeLead, "A"));
				} else if (campo.equals("NOMECID")) {
					cidade = campoImportacao.getConteudoString().trim();
					camposProspect.add(new CamposImportacao("NOMECID", "TCSPAP", cidade, "A"));
				} else {
					camposProspect.add(campoImportacao);
				}
			} else if("TCSCTT".equals(campoImportacao.getNomeTabela())) {
				if (campo.equals("TELEFONE"))
					camposContatoProspect.add(new CamposImportacao("TELEFONE", "TCSCTT", campoImportacao.getConteudoString().replace("(", "").replace(")", "").replace("-", "").replace(" ", "").trim(), "A"));
				else if (campo.equals("CELULAR"))
					camposContatoProspect.add(new CamposImportacao("CELULAR", "TCSCTT", campoImportacao.getConteudoString().replace("(", "").replace(")", "").replace("-", "").replace(" ", "").trim(), "A"));
				else
					camposContatoProspect.add(campoImportacao);
			} else if("TCSOSE".equals(campoImportacao.getNomeTabela())) {
				if(campo.equals("CODOAT"))
					codOrigem = new BigDecimal(campoImportacao.getConteudoString());
				camposNegociacao.add(campoImportacao);
			} else if("TCSITE".equals(campoImportacao.getNomeTabela())) {
				if(campo.equals("CODPROD"))
					codProd = new BigDecimal(campoImportacao.getConteudoString());
			} else {
				if (campo.equals("CANAL"))
					canal = new BigDecimal(campoImportacao.getConteudoString().trim());
			}
		}
		if(codProd == null || codProd.equals(BigDecimal.ZERO))
			throw new Exception("Produto está vazio!");
		if(!BuscarDados.buscarSeProdutoPertenceASolucao(codProd, codSolucao))
			throw new Exception("O produto informado ("+codProd+") não pertence a solução da configuração da importação ("+codSolucao+").");
		if(nomeLead == null || nomeLead.isEmpty())
			throw new Exception("Nome do lead está vazio!");
		if(cgc_cpf == null || cgc_cpf.isEmpty())
			throw new Exception("Documento do lead está vazio!");
//		if(cidade == null || cidade.isEmpty())
//			throw new Exception("Cidade do lead está vazia!");
		if(cidade != null && !cidade.isEmpty())
			camposProspect.add(new CamposImportacao("AD_CODCID", "TCSPAP", String.valueOf(buscarOuCriarCidade(cidade, codUF).intValue()), "N"));
		if(codUF == null || codUF.equals(BigDecimal.ZERO))
			throw new Exception("UF do lead está vazio!");
		if(codOrigem == null || codOrigem.equals(BigDecimal.ZERO))
			throw new Exception("Origem do lead está vazia!");
		if(codVend == null || codVend.equals(BigDecimal.ZERO))
			throw new Exception("Vendedor do lead está vazio!");
//		if(telefone == null || telefone.isEmpty())
//			throw new Exception("Telefone do lead está vazio!");
		if(validarCanalDoVendedor) {
			if(canal == null || canal.equals(BigDecimal.ZERO))
				throw new Exception("Canal está vazio!");
			if(!BuscarDados.buscarSeVendedorPertenceAoCanal(codVend, canal))
				throw new Exception("Vendedor ("+codVend+") não pertence ao canal informado ("+canal+")!");
		}
		BigDecimal codPap = ImportacaoUtils.importarProspect(camposProspect, cgc_cpf, cpf_cnpj_formatado, nroUnicoImportacao, codVend, codSolucao, msgRetorno);
		BigDecimal codContato = ImportacaoUtils.importarContato(camposContatoProspect, codPap);
		BigDecimal numOS = ImportacaoUtils.importarNegociacao(ca, cgc_cpf, cpf_cnpj_formatado, camposNegociacao, codPap, codContato, codVend, codProd, nroUnicoImportacao, codSolucao, codOrigem, msgRetorno);
		System.out.println("Dados importados:\nCód. Prospect: "+codPap+"\nCód. Contato: "+codContato+"\nNro. Negociação: "+numOS);
		System.out.println("Finalizando importação dos dados...");
	}

	private static BigDecimal buscarOuCriarBairro(String bairro) throws Exception {
		BigDecimal codBai = BuscarDados.buscarCodigoBairro(bairro);
		if(codBai == null)
			codBai = ImportacaoUtils.criarBairro(bairro);
		return codBai;
	}

	private static BigDecimal buscarOuCriarEndereco(String nomeEnd) throws Exception {
		String[] partesLogradouro = nomeEnd.split(" ");
		String tipoEndereco = partesLogradouro[0];
		String codTipoEndereco = BuscarDados.buscarCodigoTipoEndereco(tipoEndereco);
		String endereco = nomeEnd.replace(tipoEndereco, "").trim();
		BigDecimal codEnd = BuscarDados.buscarEndereco(endereco, codTipoEndereco);
		if(codEnd == null)
			codEnd = ImportacaoUtils.criarEndereco(endereco, codTipoEndereco);
		return codEnd;
	}

	private static BigDecimal buscarOuCriarCidade(String cidade, BigDecimal codUF) throws Exception {
		BigDecimal codCid = BuscarDados.buscarCidade(cidade, codUF);
		if(codCid == null)
			codCid = ImportacaoUtils.criarCidade(cidade, codUF);
		return codCid;
	}

}
