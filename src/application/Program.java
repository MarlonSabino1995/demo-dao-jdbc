package application;

import java.util.List;

import model.dao.DaoFactory;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class Program {

	public static void main(String[] args) {
		SellerDao sellerDao = DaoFactory.createSellerDao();
		System.out.println("Teste 1: selller findById");
		Seller seller = sellerDao.findById(3);
		
		System.out.println(seller);
		
		System.out.println("------------------------------------");
		System.out.println("Teste 2: department findByDepartment: ");
		Department department = new Department(2,null);
		List<Seller> list = sellerDao.findByDepartment(department);
		
		for(Seller obj : list) {
			System.out.println(obj
					);
		}
		
		System.out.println("------------------------------------");
		System.out.println("Teste 3: department findByAll: ");
		list = sellerDao.findAll();
		for(Seller obj : list) {
			System.out.println(obj);
		}
		
		
	}

}
