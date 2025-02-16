package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DBExeception;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC implements SellerDao{

	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	@Override
	public void insert(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("insert into seller(name, email, bithDate, BaseSalary, departmentId) "
					+ "values(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			
			st.setString(1, obj.getNome());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getDataDeNascimento().getTime()));
			st.setDouble(4, obj.getSalarioBase());
			st.setInt(5,obj.getDepartamento().getId());
		
			int rowsAffected = st.executeUpdate();
			
			if(rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				if(rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
					DB.closeResultSet(rs);
				}
			}
			else {
				throw new DBExeception("Error ao inserir dados ! Nenhuma linha afetada!");
			}
			
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
		}
		
		
	}

	@Override
	public void update(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE seller set name = ?, email = ?, birthDate = ?"
					+ " baseSalary = ?, department = ? where id = ?");
			
			st.setString(1, obj.getNome());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getDataDeNascimento().getTime()));
			st.setDouble(4, obj.getSalarioBase());
			st.setInt(5,obj.getDepartamento().getId());
			st.setInt(6, obj.getId());
		
			st.executeUpdate();
		
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
		}
		
	}

	@Override
	public void delete(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE FROM seller where id = ?");
			st.setInt(1, id);
			st.executeQuery();
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
		}
		
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs =null;
		try {
			st = conn.prepareStatement("SELECT seller.*,department.name as DepName "
					+ "from seller inner join department on seller.departmentId = department.id "
					+ "where seller.id = ?");
			st.setInt(1, id);
			rs = st.executeQuery();
			if(rs.next()) {
				Department dp = instantiateDepartment(rs);
				
				Seller seller = instantiateSeller(rs,dp);
				return seller ;
			}
			
			return null;
		
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instantiateSeller(ResultSet rs,Department dep) throws SQLException {
		Seller seller = new Seller();
		seller.setId(rs.getInt("Id"));;
		seller.setNome(rs.getString("Name"));
		seller.setEmail(rs.getString("Email"));
		seller.setSalarioBase(rs.getDouble("BaseSalary"));
		seller.setDataDeNascimento(rs.getDate("BirthDate"));
		seller.setDepartamento(dep);
		return seller;
	}
	private Department instantiateDepartment(ResultSet rs) throws SQLException{
		Department dp = new Department();
		dp.setId(rs.getInt("DepartmentId"));
		dp.setNome(rs.getString("name"));
		return dp;
	}
	@Override
	public List<Seller> findAll() {
		
		PreparedStatement st = null;
		ResultSet rs = null;
		
		try {
			st = conn.prepareStatement("SELECT seller.*, department.name as DepName"
					+ " from seller inner join department "
					+ "on seller.departmentid = department.id "
					+ "order by name");
			
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<>();
			
			while(rs.next()) {
				
				Department dep = map.get(rs.getInt("departmentId"));
				
				if(dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("departmentId"), null);
				}
				
				Seller obj = instantiateSeller(rs, dep);
				list.add(obj);
			}
			return list;
		}
		catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}
		finally {
			DB.closePreparedStatement(st);
			DB.closeResultSet(rs);
		}
		
	}
	@Override
	public List<Seller> findByDepartment(Department dp) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement("select seller.*, department.name as DepName "
					+ "from seller inner join department "
					+ "on seller.departmentId = department.id "
					+ "where departmentId = ? "
					+ "order by name");
			st.setInt(1, dp.getId());
			rs = st.executeQuery();
			
			List<Seller> list = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<>();
			
			while(rs.next()) {
				Department dep = map.get(rs.getInt("departmentId"));
				if(dep == null) {
					dep = instantiateDepartment(rs);
					map.put(rs.getInt("departmentID"), dep);
				}
				Seller seller = instantiateSeller(rs, dep);
				list.add(seller);
			}
			return list;
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
			DB.closeResultSet(rs);
		}
	}

}
