package pe.edu.upeu.ec.orden.repository;

import pe.edu.upeu.ec.orden.entity.Orden;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrdenRepositorio extends JpaRepository<Orden, Long> {
}
