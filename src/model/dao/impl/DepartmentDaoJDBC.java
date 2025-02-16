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
import db.DbIntegrityException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao{

	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}
	@Override
	public void insert(Department obj) {
		PreparedStatement st = null;
		
		try {
			st = conn.prepareStatement("INSERT INTO department (NAME) VALUES ?", Statement.RETURN_GENERATED_KEYS);
			st.setString(1, obj.getNome());
			
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
				throw new DBExeception("Erro ao inserir novo Departamento ! Nenhuma linha afetada! ");

			}
				
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
		}
		
	}

	@Override
	public void update(Department obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"UPDATE Dapartment set name = ? where id = ?;");
			st.setString(1, obj.getNome());
			st.setInt(2, obj.getId());
			st.executeQuery();
			
		}catch(SQLException e) {
			throw new DbIntegrityException(e.getLocalizedMessage());
		}finally {
			DB.closePreparedStatement(st);
		}
		
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"Delete from department where id = ?");
			st.setInt(1, id);
			st.executeQuery();
					
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
		}
		
	}

	@Override
	public Department findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT * from dapartment where id = ?");
			st.setInt(1, id);
			rs = st.executeQuery();
			if(rs.next()) {
				Department dp = instantiateDepartment(rs);
				return dp;
			}
			return null;
			
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
			DB.closeResultSet(rs);
		}
		
	}

	@Override
	public List<Department> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT name as Departamento FROM department ORDERE BY name",Statement.RETURN_GENERATED_KEYS);
			rs = st.executeQuery();
			
			List<Department> dep = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<Integer, Department>();
			
			while(rs.next()) {
				Department dp = map.get(rs.getInt("Department.id"));
				if(dp == null) {
					dp = instantiateDepartment(rs);
					map.put(rs.getInt("Department.id"), null);
				}
				dep.add(dp);
			}
			return dep;
			
		}catch(SQLException e) {
			throw new DBExeception(e.getMessage());
		}finally {
			DB.closePreparedStatement(st);
			DB.closeResultSet(rs);
		}
	}
	
	private Department instantiateDepartment (ResultSet rs) throws SQLException {
		Department dp = new Department();
		dp.setId(rs.getInt("department.id"));
		dp.setNome(rs.getString("name"));
		return dp;
	}

}
