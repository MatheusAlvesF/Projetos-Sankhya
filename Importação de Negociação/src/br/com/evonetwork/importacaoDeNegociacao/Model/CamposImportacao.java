package br.com.evonetwork.importacaoDeNegociacao.Model;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class CamposImportacao {
	
	private String nomeCampo;
	private String nomeTabela;
	private String conteudoString;
	private BigDecimal conteudoNumero;
	private Timestamp conteudoData;
	private String tipoConteudo;
	private String tipoImp;
	
	public CamposImportacao() {}
	
	public CamposImportacao(String nomeCampo, String nomeTabela, String conteudoString, String tipoConteudo) {
		this.nomeCampo = nomeCampo;
		this.nomeTabela = nomeTabela;
		this.conteudoString = conteudoString;
		this.tipoConteudo = tipoConteudo;
	}
	
	public String getNomeCampo() {
		return nomeCampo;
	}
	
	public void setNomeCampo(String nomeCampo) {
		this.nomeCampo = nomeCampo;
	}

	public String getConteudoString() {
		return conteudoString;
	}

	public void setConteudoString(String conteudoString) {
		this.conteudoString = conteudoString;
	}

	public BigDecimal getConteudoNumero() {
		return conteudoNumero;
	}

	public void setConteudoNumero(BigDecimal conteudoNumero) {
		this.conteudoNumero = conteudoNumero;
	}

	public String getTipoConteudo() {
		return tipoConteudo;
	}

	public void setTipoConteudo(String tipoConteudo) {
		this.tipoConteudo = tipoConteudo;
	}

	public String getNomeTabela() {
		return nomeTabela;
	}

	public void setNomeTabela(String nomeTabela) {
		this.nomeTabela = nomeTabela;
	}

	public Timestamp getConteudoData() {
		return conteudoData;
	}

	public void setConteudoData(Timestamp conteudoData) {
		this.conteudoData = conteudoData;
	}

	public String getTipoImp() {
		return tipoImp;
	}

	public void setTipoImp(String tipoImp) {
		this.tipoImp = tipoImp;
	}
	
}
