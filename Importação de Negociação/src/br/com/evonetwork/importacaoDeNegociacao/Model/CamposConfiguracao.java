package br.com.evonetwork.importacaoDeNegociacao.Model;

import java.math.BigDecimal;

public class CamposConfiguracao {
	
	private BigDecimal coluna;
	private String tabela;
	private String nome;
	private String tipo;
	private BigDecimal qtdDecimais;
	
	public CamposConfiguracao() {}
	
	public BigDecimal getColuna() {
		return coluna;
	}
	
	public void setColuna(BigDecimal coluna) {
		this.coluna = coluna;
	}
	
	public String getTabela() {
		return tabela;
	}
	
	public void setTabela(String tabela) {
		this.tabela = tabela;
	}
	
	public String getNome() {
		return nome;
	}
	
	public void setNome(String nome) {
		this.nome = nome;
	}
	
	public String getTipo() {
		return tipo;
	}
	
	public void setTipo(String tipo) {
		this.tipo = tipo;
	}

	public BigDecimal getQtdDecimais() {
		return qtdDecimais;
	}

	public void setQtdDecimais(BigDecimal qtdDecimais) {
		this.qtdDecimais = qtdDecimais;
	}
	
}
