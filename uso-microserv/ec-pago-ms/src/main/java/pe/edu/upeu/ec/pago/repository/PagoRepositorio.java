package pe.edu.upeu.ec.pago.repository;

import pe.edu.upeu.ec.pago.entity.Pago;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PagoRepositorio extends JpaRepository<Pago, Long> {
}
