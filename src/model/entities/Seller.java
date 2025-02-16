package model.entities;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class Seller implements Serializable{

	private static final long serialVersionUID = 1L;

	public static SimpleDateFormat sdfm = new SimpleDateFormat("dd/MM/yyyy");
	private Integer id;
	private String nome;
	private String email;
	private Date dataDeNascimento;
	private Double salarioBase;
	
	private Department departamento;

	public Seller() {
	}

	public Seller(Integer id, String nome, String email, Date dataDeNascimento, Double salarioBase,
			Department departamento) {
		this.id = id;
		this.nome = nome;
		this.email = email;
		this.dataDeNascimento =  dataDeNascimento;
		this.salarioBase = salarioBase;
		this.departamento = departamento;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getNome() {
		return nome;
	}

	public void setNome(String nome) {
		this.nome = nome;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public Date getDataDeNascimento() {
		return dataDeNascimento;
	}

	public void setDataDeNascimento(Date dataDeNascimento) {
		this.dataDeNascimento = dataDeNascimento;
	}

	public Double getSalarioBase() {
		return salarioBase;
	}

	public void setSalarioBase(Double salarioBase) {
		this.salarioBase = salarioBase;
	}

	public Department getDepartamento() {
		return departamento;
	}

	public void setDepartamento(Department departamento) {
		this.departamento = departamento;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Seller other = (Seller) obj;
		return Objects.equals(id, other.id);
	}

	@Override
	public String toString() {
		return "Seller [id=" + id + ", nome=" + nome + ", email=" + email + ", dataDeNascimento=" + dataDeNascimento
				+ ", salarioBase=" + salarioBase + ", departamento=" + departamento + "]";
	}
	
	
}
