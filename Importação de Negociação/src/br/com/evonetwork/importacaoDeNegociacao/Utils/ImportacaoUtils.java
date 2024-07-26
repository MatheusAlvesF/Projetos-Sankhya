package br.com.evonetwork.importacaoDeNegociacao.Utils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;

import com.sankhya.util.TimeUtils;

import br.com.evonetwork.importacaoDeNegociacao.Model.CamposImportacao;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.bmp.PersistentLocalEntity;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.MGECoreParameter;

public class ImportacaoUtils {

	public static BigDecimal criarBairro(String bairro) throws Exception {
		BigDecimal codBairro = null;
		JapeSession.SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			JapeWrapper empresaDAO = JapeFactory.dao(DynamicEntityNames.BAIRRO);
			DynamicVO save = empresaDAO.create()
				.set("NOMEBAI", bairro)
				.save();
			codBairro = save.asBigDecimal("CODBAI");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JapeSession.close(hnd);
		}
		return codBairro;
	}

	public static BigDecimal importarProspect(ArrayList<CamposImportacao> camposProspect, String cgc_cpf, String cpf_cnpj_formatado, BigDecimal nroUnicoImportacao, BigDecimal codVend, BigDecimal codSolucao, StringBuilder msgRetorno) throws Exception {
		System.out.println("Iniciando inclusão prospect...");
		BigDecimal codPap = BuscarDados.buscarCodigoProspect(cgc_cpf);
		BigDecimal vendedorPadrao = new BigDecimal(MGECoreParameter.getParameterAsInt("VENDPADRAOCRMUN")); //VENDEDOR PADRÃO
		if(codPap == null) {
			System.out.println("Não existe prospect com o CPF/CNPJ informado, criando um novo...");
			EntityFacade dwfFacadeP = EntityFacadeFactory.getDWFFacade();
			DynamicVO dynamicPAP = (DynamicVO) dwfFacadeP.getDefaultValueObjectInstance(DynamicEntityNames.PARCEIRO_PROSPECT);	
			dynamicPAP.setProperty("AD_NROUNICO", nroUnicoImportacao);
			for (CamposImportacao campoProspect : camposProspect) {
				String nomeCampo = campoProspect.getNomeCampo();
				String conteudoCampo = campoProspect.getConteudoString().trim();
				String tipoCampo = campoProspect.getTipoConteudo();
				System.out.println("Adicionando campo: '"+nomeCampo+"' - '"+tipoCampo+"' - '"+conteudoCampo+"'.");
				switch (tipoCampo) {
					case "A":
						dynamicPAP.setProperty(nomeCampo, conteudoCampo);
						break;
					case "N":
						dynamicPAP.setProperty(nomeCampo, new BigDecimal(conteudoCampo));
						break;
					case "D":
						System.out.println("Campo tipo data não configurado para importação!");
				}
			}
			if (cgc_cpf.length() > 11) {
				dynamicPAP.setProperty("TIPPESSOA","J");
				System.out.println("Tipo Pessoa: J");
			} else {
				dynamicPAP.setProperty("TIPPESSOA","F");
				System.out.println("Tipo Pessoa: F");
			}
			dynamicPAP.setProperty("CODVEND", vendedorPadrao);
			PersistentLocalEntity createEntity = dwfFacadeP.createEntity(DynamicEntityNames.PARCEIRO_PROSPECT,(EntityVO) dynamicPAP);
			DynamicVO save = (DynamicVO) createEntity.getValueObject();
			codPap = new BigDecimal(save.getProperty("CODPAP").toString());
			System.out.println("Prospect incluido com sucesso: "+codPap);
			criarVendedorTipoSolucaoParaProspect(codPap, codVend, codSolucao);
		} else {
			System.out.println("Existe prospect com o CPF/CNPJ informado, atualizando os dados...");
			System.out.println("Prospect existente: "+codPap);
			String nomePap = null;
			String razaoSocial = null;
			String email = null;
			String endereco = null;
			String nomeCid = null;
			BigDecimal codUf = null;
			BigDecimal codOat = null;
			String telefone = null;
			BigDecimal codBai = null;
			BigDecimal codEnd = null;
			BigDecimal codCid = null;
			String numeroEnd = null;
			for (CamposImportacao campoProspect : camposProspect) {
				String nomeCampo = campoProspect.getNomeCampo();
				String conteudoCampo = campoProspect.getConteudoString().trim();
				switch (nomeCampo) {
					case "NOMEPAP":
						nomePap = conteudoCampo;
						break;
					case "RAZAOSOCIAL":
						razaoSocial = conteudoCampo;
						break;
					case "EMAIL":
						email = conteudoCampo;
						break;
					case "ENDERECO":
						endereco = conteudoCampo;
						break;
					case "NUMEND":
						numeroEnd = conteudoCampo;
						break;
					case "AD_CODBAI":
						codBai = new BigDecimal(conteudoCampo);
						break;
					case "NOMECID":
						nomeCid = conteudoCampo;
						break;
					case "CODUF":
						codUf = new BigDecimal(conteudoCampo);
						break;
					case "TELEFONE":
						telefone = conteudoCampo;
						break;
					case "AD_CODOAT":
						codOat = new BigDecimal(conteudoCampo);
						break;
					case "AD_CODEND":
						codEnd = new BigDecimal(conteudoCampo);
						break;
					case "AD_CODCID":
						codCid = new BigDecimal(conteudoCampo);
						break;
				}
			}
			BigDecimal numOS = BuscarDados.buscarNegociacaoAbertaParaSolucao(codPap, codSolucao);
			boolean existeNegociacaoAbertaParaSolucao = (numOS != null && numOS.compareTo(BigDecimal.ZERO) > 0);
			if(existeNegociacaoAbertaParaSolucao && BuscarDados.buscarSeOrigemEhElegivelParaAtualizar(codOat)) {
				atualizarOrigemDoProspect(codPap, codOat, nroUnicoImportacao);
				atualizarOrigemDaNegociacao(numOS, codOat);
				throw new Exception("Prospect já estava cadastrado e origem está cadastrada na tela 'Origens Elegíveis para Atualização', apenas a origem foi atualizada. Código: "+codPap+"");
			}
			//se os campos abaixo estiverem vazios na planilha, não pode remover o valor atual
			if(email == null || email.isEmpty()) 
				email = BuscarDados.buscarCampoTextoDoProspectExistente("EMAIL", codPap);
			if(endereco == null || endereco.isEmpty())
				endereco = BuscarDados.buscarCampoTextoDoProspectExistente("ENDERECO", codPap);
			if(numeroEnd == null || numeroEnd.isEmpty())
				numeroEnd = BuscarDados.buscarCampoTextoDoProspectExistente("NUMEND", codPap);
			if(codBai == null || codBai.compareTo(BigDecimal.ZERO) == 0)
				codBai = BuscarDados.buscarCampoInteiroDoProspectExistente("AD_CODBAI", codPap);
			if(codEnd == null || codEnd.compareTo(BigDecimal.ZERO) == 0) 
				codEnd = BuscarDados.buscarCampoInteiroDoProspectExistente("AD_CODEND", codPap);
			if(telefone == null || telefone.isEmpty()) 
				telefone = BuscarDados.buscarCampoTextoDoProspectExistente("TELEFONE", codPap);
			if(codCid == null || codCid.compareTo(BigDecimal.ZERO) == 0) 
				codCid = BuscarDados.buscarCampoInteiroDoProspectExistente("AD_CODCID", codPap);
			SessionHandle hnd = null;
			try {
				hnd = JapeSession.open();
				JapeWrapper prospectDAO = JapeFactory.dao(DynamicEntityNames.PARCEIRO_PROSPECT);
				prospectDAO.prepareToUpdateByPK(codPap)
					.set("AD_NROUNICOVINC", nroUnicoImportacao)
//					.set("AD_PREVENDEDOR", preVendedor)
					.set("CODVEND", vendedorPadrao) //OBRIGATÓRIO
//					.set("AD_CODUSUNEG", usuarioPadrao)
					.set("NOMEPAP", nomePap) //OBRIGATÓRIO
					.set("RAZAOSOCIAL", razaoSocial) //OBRIGATÓRIO
					.set("EMAIL", email)
					.set("ENDERECO", endereco)
					.set("NOMECID", nomeCid) //OBRIGATÓRIO
					.set("AD_CODBAI", codBai)
					.set("AD_CODEND", codEnd)
					.set("AD_CODCID", codCid) //OBRIGATÓRIO
					.set("NUMEND", numeroEnd)
//					.set("CEP", cep)
					.set("CODUF", codUf) //OBRIGATÓRIO
//					.set("AD_EXACTSALES", exactSales)
//					.set("AD_PREVENDEDOR", preVendedor)
//					.set("AD_DESCORIGEMATEND", descOrigAtend)
					.set("AD_CODOAT", codOat) //OBRIGATÓRIO
//					.set("AD_FATURA", fatura)
					.set("TELEFONE", telefone) //OBRIGATÓRIO
//					.set("AD_INSTALACAOMANUAL", instManual)
//					.set("AD_CONSULTARECSEF", consulta)
					.update();
				if(BuscarDados.buscarSeExisteEsteVendedorNoProspect(codPap, codSolucao)) {
					if(!existeNegociacaoAbertaParaSolucao) {
						alterarVendedorTipoSolucaoParaProspect(codPap, codVend, codSolucao);
					} else {
						throw new Exception("Já existe negociação aberta para esse prospect! Não é permitido alterar vendedor.");
					}
				} else {
					criarVendedorTipoSolucaoParaProspect(codPap, codVend, codSolucao);
				}
			} finally {
				JapeSession.close(hnd);
			}
			msgRetorno.append("<b>CPF/CNPJ "+cpf_cnpj_formatado+":</b> Prospect já estava cadastrado, código: "+codPap+"<br/>");
		}
		System.out.println("Prospect criado/atualizado: "+codPap);
		return codPap;
	}

	private static void atualizarOrigemDoProspect(BigDecimal codPap, BigDecimal codOat, BigDecimal nroUnicoImportacao) throws Exception {
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			JapeWrapper prospectDAO = JapeFactory.dao(DynamicEntityNames.PARCEIRO_PROSPECT);
			prospectDAO.prepareToUpdateByPK(codPap)
				.set("AD_NROUNICOVINC", nroUnicoImportacao)
				.set("AD_CODOAT", codOat)
				.update();
		} finally {
			JapeSession.close(hnd);
		}
	}

	private static void atualizarOrigemDaNegociacao(BigDecimal numOS, BigDecimal codOat) throws Exception {
		SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			JapeWrapper ordemServicoDAO = JapeFactory.dao(DynamicEntityNames.ORDEM_SERVICO);
			ordemServicoDAO.prepareToUpdateByPK(numOS)
				.set("CODOAT", codOat)
				.update();
		} finally {
			JapeSession.close(hnd);
		}
	}

	private static void alterarVendedorTipoSolucaoParaProspect(BigDecimal codPap, BigDecimal codVend, BigDecimal codSolucao) throws Exception {
		JapeSession.SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			JapeWrapper vendSolucaoDAO = JapeFactory.dao("AD_VENDTIPOSOLUCAOPROSP");
			DynamicVO vendSolucaoVO = vendSolucaoDAO.findOne("CODPAP = "+codPap+" AND CODSOL = "+codSolucao);
			vendSolucaoDAO.prepareToUpdate(vendSolucaoVO)
				.set("CODVEND", codVend)
				.update();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JapeSession.close(hnd);
		}
	}

	private static void criarVendedorTipoSolucaoParaProspect(BigDecimal codPap, BigDecimal codVend, BigDecimal codSolucao) throws Exception {
		JapeSession.SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setCanTimeout(false);
            hnd.setPriorityLevel(JapeSession.LOW_PRIORITY);//em casos de deadlock, esta sessao cai
			JapeWrapper empresaDAO = JapeFactory.dao("AD_VENDTIPOSOLUCAOPROSP");
			@SuppressWarnings("unused")
			DynamicVO save = empresaDAO.create()
				.set("CODPAP", codPap)
				.set("CODVEND", codVend)
				.set("CODSOL", codSolucao)
				.save();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JapeSession.close(hnd);
		}
	}

	public static BigDecimal importarContato(ArrayList<CamposImportacao> camposContatoProspect, BigDecimal codPap) throws Exception {
		System.out.println("Iniciando inclusão contato para o prospect "+codPap+"...");
		EntityFacade dwfFacadeP = EntityFacadeFactory.getDWFFacade();
		DynamicVO dynamicCTT = (DynamicVO) dwfFacadeP.getDefaultValueObjectInstance(DynamicEntityNames.CONTATO_PROSPECT);	
		dynamicCTT.setProperty("CODPAP", codPap);
		for (CamposImportacao campoContato : camposContatoProspect) {
			String nomeCampo = campoContato.getNomeCampo();
			String conteudoCampo = campoContato.getConteudoString().trim();
			String tipoCampo = campoContato.getTipoConteudo();
			System.out.println("Adicionando campo: '"+nomeCampo+"' - '"+tipoCampo+"' - '"+conteudoCampo+"'.");
			switch (tipoCampo) {
				case "A":
					dynamicCTT.setProperty(nomeCampo, conteudoCampo);
					break;
				case "N":
					dynamicCTT.setProperty(nomeCampo, new BigDecimal(conteudoCampo));
					break;
				case "D":
					System.out.println("Campo tipo data não configurado para importação!");
			}
		}
		PersistentLocalEntity createEntityCTT = dwfFacadeP.createEntity(DynamicEntityNames.CONTATO_PROSPECT,(EntityVO) dynamicCTT);
		DynamicVO saveCTT = (DynamicVO) createEntityCTT.getValueObject();
		BigDecimal codContato = (BigDecimal) saveCTT.getProperty("CODCONTATO");
		System.out.println("Contato criado: "+codContato);
		return codContato;
	}

	public static BigDecimal importarNegociacao(ContextoAcao ca, String cgc_cpf, String cpf_cnpj_formatado, ArrayList<CamposImportacao> camposNegociacao, BigDecimal codPap, BigDecimal codContato, BigDecimal codVend, BigDecimal codProd, BigDecimal nroUnicoImportacao, BigDecimal codSolucao, BigDecimal codOat, StringBuilder msgRetorno) {
		BigDecimal numOS = BigDecimal.ZERO;
		Collection<Object> paramIn 	= new ArrayList<Object>();
		Collection<String> paramOut	= new ArrayList<String>();
		paramIn.add(nroUnicoImportacao);
		QueryExecutor query = ca.getQuery();
		try {
			System.out.println("Executando procedure STP_VINC_PROSP_MATRIZ_ALSOL {"+paramIn+", "+paramOut+"}");
			query.executeProcedure("STP_VINC_PROSP_MATRIZ_ALSOL", paramIn, paramOut);
			query.close();
			BigDecimal codUsuVend = BuscarDados.buscarUsuarioDoVendedor(codVend);
			int negRelacInic = BuscarDados.buscarNegociacaoRelacionadaInicio(cgc_cpf, nroUnicoImportacao);
			int negRelacOutr = BuscarDados.buscarNegociacaoRelacionadaOutros(cgc_cpf, nroUnicoImportacao);
			BigDecimal codPapMatriz = BuscarDados.buscarProspectMatriz(cgc_cpf, nroUnicoImportacao);
			BigDecimal vendedorPadrao = new BigDecimal(MGECoreParameter.getParameterAsInt("VENDPADRAOCRMUN")); //VENDEDOR PADRÃO
			BigDecimal codUsuVendPadrao = BuscarDados.buscarUsuarioDoVendedor(vendedorPadrao);
			if (negRelacInic > 0) {
				QueryExecutor upd = ca.getQuery();
				StringBuilder sql = new StringBuilder();
				sql = new StringBuilder();
				sql.append("UPDATE TCSPAP ");
				sql.append("SET AD_CODPAPMATRIZ = "+codPapMatriz+" ");
				sql.append("WHERE AD_NROUNICO =  "+nroUnicoImportacao+" AND (CODPAP = "+codPap+" OR AD_CODPAPMATRIZ = "+codPap+") ");
				System.out.println("UPD: "+sql.toString());
				upd.update(sql.toString());
				upd.close();
				msgRetorno.append("<b>CPF/CNPJ "+cpf_cnpj_formatado+":</b> Vinculado a negociação do prospect "+codPapMatriz+"<br/>");
			} else {
				try {
					//---------------------------------------------------------------------------------------------------------
					//Inclusão cabeçalho negocação
					System.out.println("Criando negociação...");
					EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
					DynamicVO dynamicOSE = (DynamicVO) dwfFacade.getDefaultValueObjectInstance(DynamicEntityNames.ORDEM_SERVICO);	
					BigDecimal codMetod = BuscarDados.buscarCodigoModeloDoProduto(codProd);
					dynamicOSE.setProperty("CODPARC", BigDecimal.ZERO);
					dynamicOSE.setProperty("CODPAP", codPap);
					dynamicOSE.setProperty("CODCONTATOPAP", codContato);
					dynamicOSE.setProperty("CODVEND", vendedorPadrao);
					dynamicOSE.setProperty("DHCHAMADA", TimeUtils.getNow());
					dynamicOSE.setProperty("CODOAT", codOat);
					dynamicOSE.setProperty("CODTPN", BigDecimal.ONE);
					dynamicOSE.setProperty("CODATEND", codUsuVendPadrao);
					dynamicOSE.setProperty("CODUSURESP", codUsuVendPadrao);
					dynamicOSE.setProperty("SITUACAO", "P");
					dynamicOSE.setProperty("TIPO", "P");
					dynamicOSE.setProperty("AD_NROUNICO", nroUnicoImportacao);
					dynamicOSE.setProperty("AD_STATUSINTEGRACAO", "D");
					dynamicOSE.setProperty("CODMETOD", codMetod);
					PersistentLocalEntity createEntityOSE = dwfFacade.createEntity(DynamicEntityNames.ORDEM_SERVICO,(EntityVO) dynamicOSE);
					DynamicVO saveOSE = (DynamicVO) createEntityOSE.getValueObject();
					numOS = new BigDecimal(saveOSE.getProperty("NUMOS").toString());
					System.out.println("Negociação criada: "+numOS);
					QueryExecutor queryUpdOSE = ca.getQuery();
					String strUpd = "UPDATE TCSOSE SET CODVEND = "+codVend+", CODATEND = "+codUsuVend+", CODUSURESP = "+codUsuVend+" WHERE NUMOS = "+numOS;
					System.out.println("UPD: "+strUpd);
                    queryUpdOSE.update(strUpd);
                    queryUpdOSE.close();
					//Fim cabeçalho negociação
					//---------------------------------------------------------------------------------------------------------
		
					//---------------------------------------------------------------------------------------------------------
					//Inclusão item negocação
                    System.out.println("Criando item negociação...");
                    BigDecimal codServ = BuscarDados.buscarEtapaDoProdutoPelaSequencia(codProd, 1);
                    BigDecimal codServ2 = BuscarDados.buscarEtapaDoProdutoPelaSequencia(codProd, 2);
                    BigDecimal codSit = BuscarDados.buscarCodigoSituacao(codServ);
                    BigDecimal codSit2 = BuscarDados.buscarCodigoSituacao(codServ2);
                    BigDecimal codOcorOS = BuscarDados.buscarMotivoDoServico(codServ);
                    BigDecimal codOcorOS2 = BuscarDados.buscarMotivoDoServico(codServ2);
                    BigDecimal codEpv = BigDecimal.valueOf(6);
                    Timestamp dtatual = TimeUtils.getNow();
					dwfFacade = EntityFacadeFactory.getDWFFacade();
					DynamicVO dynamicITE = (DynamicVO) dwfFacade.getDefaultValueObjectInstance(DynamicEntityNames.ITEM_ORDEM_SERVICO);
					dynamicITE.setProperty("NUMOS", numOS);
					dynamicITE.setProperty("CODPROD", codProd);
					if (negRelacOutr > 0 ) {
						dynamicITE.setProperty("CODSERV", codServ2);
						dynamicITE.setProperty("CODOCOROS", codOcorOS2);
						dynamicITE.setProperty("CODSIT", codSit2);
					} else {
						dynamicITE.setProperty("CODSERV", codServ);
						dynamicITE.setProperty("CODOCOROS", codOcorOS);
						dynamicITE.setProperty("CODSIT", codSit);
					}
					dynamicITE.setProperty("CODUSU", codUsuVend);
					dynamicITE.setProperty("TEMPPREVISTO", dtatual);
					dynamicITE.setProperty("VLRCOBRADO", BigDecimal.ZERO);
					dynamicITE.setProperty("CODEPV", codEpv);
					dynamicITE.setProperty("DHPREVISTA", TimeUtils.getNow());
					PersistentLocalEntity createEntityITE = dwfFacade.createEntity(DynamicEntityNames.ITEM_ORDEM_SERVICO,(EntityVO) dynamicITE);
					DynamicVO saveITE = (DynamicVO) createEntityITE.getValueObject();
					BigDecimal numItem = new BigDecimal(saveITE.getProperty("NUMITEM").toString());
					System.out.println("Item criado: "+numItem);
					//Fim item negociação
					//---------------------------------------------------------------------------------------------------------
					
					//------------------------------------------------------------------------------------
					//Atualizar primeira etapa para segunda etapa
					System.out.println("Atualizando negociação para segunda etapa...");
					BigDecimal hora = new BigDecimal(TimeUtils.formataHHMM(dtatual));
					System.out.println("Fechando primeira etapa da negociação: "+numOS+" - Item: "+numItem);
					SessionHandle hnd = null;
					try {
						hnd = JapeSession.open();
						JapeFactory.dao(DynamicEntityNames.ITEM_ORDEM_SERVICO).prepareToUpdateByPK(new Object[] {numOS, numItem})
							.set("INICEXEC", dtatual)
							.set("HRINICIAL", hora)
							.set("HRFINAL", hora)
							.set("CODSIT", new BigDecimal(6))
							.set("SOLUCAO", "")
							.update();
					} catch (Exception e) {
						e.printStackTrace();
						throw new Exception(e.getMessage());
					} finally {
						JapeSession.close(hnd);
					}
					
					//Inserindo na etapa 2
					System.out.println("Inserindo etapa 2...");
					JapeSession.SessionHandle hnd2 = null;
					BigDecimal graudific = BigDecimal.ZERO;
					Timestamp umMes = TimeUtils.dataAddDay(dtatual, 30);
					String retrabalho = "N";
					try {
						hnd2 = JapeSession.open();
						JapeWrapper InsertITE = JapeFactory.dao(DynamicEntityNames.ITEM_ORDEM_SERVICO);
						DynamicVO salvar = InsertITE.create()
								.set("NUMOS", numOS)
								.set("CODSERV", codServ2)
								.set("CODPROD", codProd)
								.set("GRAUDIFIC", graudific)
								.set("RETRABALHO", retrabalho)
								.set("CODUSU", codUsuVend)
								.set("DHPREVISTA", dtatual)
								.set("CODOCOROS", codOcorOS2)
								.set("CODSIT", codSit)
								.set("CODEPV", codEpv)
								.set("DTPREVFECHAMENTO", umMes)
								.set("VLRCOBRADO", BigDecimal.ZERO)
								.save();
						System.out.println("Item criado: "+salvar.asBigDecimal("NUMITEM"));
					} catch (Exception e) {
						ca.mostraErro(e.getMessage());
					} finally {
						JapeSession.close(hnd2);
					}
					//------------------------------------------------------------------------------------
					
					//------------------------------------------------------------------------------------
					//Preencher campanha padrão para Fontes Renováveis
					if(codSolucao.compareTo(BigDecimal.ONE) == 0) { 
						QueryExecutor upd = ca.getQuery();
						StringBuilder sql = new StringBuilder();
						sql.append("UPDATE TCSOSE SET AD_NROITEMCAMPANHA = (SELECT INTEIRO FROM TSIPAR WHERE CHAVE = 'CAMPNEGPROS') WHERE NUMOS = "+numOS);
						System.out.println("UPD: "+sql.toString());
						upd.update(sql.toString());
						upd.close();
					}
					//------------------------------------------------------------------------------------
				} catch (Exception e) {
					if (numOS.compareTo(BigDecimal.ZERO) > 0) {
						SessionHandle hnd = null;
						try {
							hnd = JapeSession.open();
							EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
							dwfFacade.findEntityByPrimaryKey(DynamicEntityNames.ORDEM_SERVICO,numOS).remove();
						} finally {
							JapeSession.close(hnd);
						}
					}
					msgRetorno.append("<b>CPF/CNPJ "+cpf_cnpj_formatado+":</b> "+e.getMessage()+"<br/>");
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			msgRetorno.append("<b>CPF/CNPJ "+cpf_cnpj_formatado+":</b> "+e.getMessage()+"<br/>");
			e.printStackTrace();
		}
		return numOS;
	}

	public static BigDecimal criarEndereco(String endereco, String codTipoEndereco) throws Exception {
		BigDecimal codEnd = null;
		JapeSession.SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setCanTimeout(false);
            hnd.setPriorityLevel(JapeSession.LOW_PRIORITY);//em casos de deadlock, esta sess o cai
			JapeWrapper empresaDAO = JapeFactory.dao("Endereco");
			DynamicVO save = empresaDAO.create()
				.set("NOMEEND", endereco)
				.set("TIPO", codTipoEndereco)
				.save();
			codEnd = save.asBigDecimal("CODEND");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JapeSession.close(hnd);
		}
		return codEnd;
	}

	public static BigDecimal criarCidade(String cidade, BigDecimal codUF) throws Exception {
		BigDecimal codCid = null;
		JapeSession.SessionHandle hnd = null;
		try {
			hnd = JapeSession.open();
			hnd.setCanTimeout(false);
            hnd.setPriorityLevel(JapeSession.LOW_PRIORITY);//em casos de deadlock, esta sess o cai
			JapeWrapper empresaDAO = JapeFactory.dao("Cidade");
			DynamicVO save = empresaDAO.create()
				.set("NOMECID", cidade)
				.set("UF", codUF)
				.save();
			codCid = save.asBigDecimal("CODCID");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		} finally {
			JapeSession.close(hnd);
		}
		return codCid;
	}

}
